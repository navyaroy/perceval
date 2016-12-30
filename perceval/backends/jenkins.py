# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#

import json
import csv
import sys
import logging
import os.path

import requests

from ..backend import Backend, BackendCommand, metadata
from ..cache import Cache
from ..errors import CacheError
from ..utils import (DEFAULT_DATETIME,
                     datetime_to_utc,
                     str_to_datetime,
                     urljoin)


logger = logging.getLogger(__name__)


class Jenkins(Backend):
    """Jenkins backend for Perceval.

    This class retrieves the builds from a Jenkins site.
    To initialize this class the URL must be provided.
    The `url` will be set as the origin of the data.

    :param url: Jenkins url
    :param tag: label used to mark the data
    :param cache: cache object to store raw data
    :param blacklist_jobs: exclude the jobs of this list while fetching
    """
    version = '0.5.0'

    def __init__(self, url, tag=None, cache=None, blacklist_jobs=None):
        origin = url

        super().__init__(origin, tag=tag, cache=cache)
        self.url = url
        self.client = JenkinsClient(url, blacklist_jobs)
        self.blacklist_jobs = blacklist_jobs

    @metadata
    def fetch(self):
        """Fetch the builds from the url.

        The method retrieves, from a Jenkins url, the
        builds updated since the given date.

        :returns: a generator of builds
        """

        logger.info("Looking for projects at url '%s'", self.url)

        self._purge_cache_queue()
        nbuilds = 0  # number of builds processed
        njobs = 0 # number of jobs processed

        projects = json.loads(self.client.get_jobs())
        jobs = projects['jobs']

        for job in jobs:
            njobs += 1
            logger.debug("Adding builds from %s (%i/%i)", job['url'], njobs, len(jobs))
            raw_builds = self.client.get_builds(job['name'])
            if not raw_builds:
                continue
            self._push_cache_queue(raw_builds)
            self._flush_cache_queue()
            builds = json.loads(raw_builds)
            builds = builds['builds']
            for build in builds:
                yield build
                nbuilds += 1

        logger.info("Total number of jobs: %i" % (njobs))
        logger.info("Total number of builds: %i" % (nbuilds))

    @metadata
    def fetch_from_cache(self):
        """Fetch the builds from the cache.

        :returns: a generator of builds

        :raises CacheError: raised when an error occurs accessing the
            cache
        """
        if not self.cache:
            raise CacheError(cause="cache instance was not provided")

        cache_items = self.cache.retrieve()

        for items in cache_items:
            builds = json.loads(items)['builds']
            for build in builds:
                yield build

    @classmethod
    def has_caching(cls):
        """Returns whether it supports caching items on the fetch process.

        :returns: this backend supports items cache
        """
        return True

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend does not supports items resuming
        """
        return False

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Build item."""
        return str(item['url'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Jenkins item.

        The timestamp is extracted from 'timestamp' field.
        This date is a UNIX timestamp but needs to be converted to
        a float value.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        return float(item['timestamp']/1000)

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Jenkins item.

        This backend only generates one type of item which is
        'build'.
        """
        return 'build'


class JenkinsClient:
    """Jenkins API client.

    This class implements a simple client to retrieve builds from
    projects in a Jenkins node.

    :param url: URL of jenkins node: https://build.opnfv.org/ci

    :raises HTTPError: when an error occurs doing the request
    """

    def __init__(self, url, blacklist_jobs=None):
        self.url = url
        self.blacklist_jobs = blacklist_jobs

    def get_jobs(self):
        """ Retrieve all jobs
        """
        url_jenkins = self.url + "/view/All/api/json"

        req = requests.get(url_jenkins)
        req.raise_for_status()
        return req.text

    def get_builds(self, job_name):
        """ Retrieve all builds from a job
        """

        if self.blacklist_jobs and job_name in self.blacklist_jobs:
            logging.info("Not getting blacklisted job: %s", job_name)
            return

        # depth=2 to get builds details
        job_url = self.url + "/job/%s/" % (job_name)
        url_jenkins = job_url + "api/json?depth=2"

        req = requests.get(url_jenkins)
        req.raise_for_status()
        return req.text

class JenkinsCommand(BackendCommand):
    """Class to run Jenkins backend from the command line."""

    def __init__(self, *args):
        super().__init__(*args)
        self.url = self.parsed_args.url
        self.tag = self.parsed_args.tag
        self.outfile = self.parsed_args.outfile
        self.blacklist_jobs = self.parsed_args.blacklist_jobs
        self.isCsv = self.parsed_args.csv_format

        if not self.parsed_args.no_cache:
            if not self.parsed_args.cache_path:
                base_path = os.path.expanduser('~/.perceval/cache/')
            else:
                base_path = self.parsed_args.cache_path

            cache_path = os.path.join(base_path, self.url)

            cache = Cache(cache_path)

            if self.parsed_args.clean_cache:
                cache.clean()
            else:
                cache.backup()
        else:
            cache = None

        self.backend = Jenkins(self.url, tag=self.tag, cache=cache,
                               blacklist_jobs=self.blacklist_jobs)

    def CSVformatOutput(self, commits):
        try:
            if self.outfile.name == '<stdout>':
                fileCVS = csv.writer( sys.stdout)
            else:
                fileCVS = csv.writer(open(self.outfile.name, "w+"))

            fileCVS.writerow(["Backend_name", "Backend_version",
                              "Origin", "Perceval_version", "Tag",
                              "Timestamp", "Updated_on", "Uuid"])
            for commit in commits:
                string = json.dumps(commit, indent=4, sort_keys=True)
                obj = json.loads(string)

                fileCVS.writerow([obj["backend_name"],
                                  obj["backend_version"],
                                  obj["origin"],
                                  obj["perceval_version"],
                                  obj["tag"],
                                  obj["timestamp"],
                                  obj["updated_on"],
                                  obj["uuid"]])

        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(str(e.response.json()))
        except IOError as e:
            raise RuntimeError(str(e))
        except Exception as e:
            if self.backend.cache:
                self.backend.cache.recover()
            raise RuntimeError(str(e))


    def JSONformatOutput(self, commits):
        try:
            for commit in commits:
                obj = json.dumps(commit, indent=4, sort_keys=True)
                self.outfile.write(obj)
                self.outfile.write('\n')
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(str(e.response.json()))
        except IOError as e:
            raise RuntimeError(str(e))
        except Exception as e:
            if self.backend.cache:
                self.backend.cache.recover()
            raise RuntimeError(str(e))

    def run(self):
        """Fetch and print the Builds.

        This method runs the backend to fetch the Builds of a given url.
        Builds are converted to JSON objects and printed to the
        defined output.
        """
        if self.parsed_args.fetch_cache:
            builds = self.backend.fetch_from_cache()
        else:
            builds = self.backend.fetch()

        if self.isCsv:
            self.CSVformatOutput( builds )
        else:
            self.JSONformatOutput( builds )

    @classmethod
    def create_argument_parser(cls):
        """Returns the Jenkins argument parser."""

        parser = super().create_argument_parser()

        # Jenkins options
        group = parser.add_argument_group('Jenkins arguments')

        group.add_argument("--blacklist-jobs",  dest="blacklist_jobs",
                           nargs='*', help="Wrong jobs that must not be retrieved.")

        # Required arguments
        group.add_argument('url',
                           help="URL of the Jenkins server")

        return parser
