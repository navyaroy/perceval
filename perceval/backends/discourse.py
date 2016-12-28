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
#    Santiago Dueñas <sduenas@bitergia.com>
#    J. Manrique López de la Fuente <jsmanrique@bitergia.com>
#    Alvaro del Castillo San Felix <acs@bitergia.com>
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


class Discourse(Backend):
    """Discourse backend for Perceval.

    This class retrieves the topics posted in a Discourse board.
    To initialize this class the URL must be provided. The `url`
    will be set as the origin of the data.

    :param url: Discourse URL
    :param token: Discourse API access token
    :param tag: label used to mark the data
    :param cache: cache object to store raw data
    """
    version = '0.4.0'

    def __init__(self, url, token=None,
                 tag=None, cache=None):
        origin = url

        super().__init__(origin, tag=tag, cache=cache)
        self.url = url
        self.client = DiscourseClient(url, api_key=token)

    @metadata
    def fetch(self, from_date=DEFAULT_DATETIME):
        """Fetch the topics from the Discurse board.

        The method retrieves, from a Discourse board the topics
        updated since the given date.

        :param from_date: obtain topics updated since this date

        :returns: a generator of topics
        """
        if not from_date:
            from_date = DEFAULT_DATETIME
        else:
            from_date = datetime_to_utc(from_date)

        logger.info("Looking for topics at '%s', updated from '%s'",
                    self.url, str(from_date))

        self._purge_cache_queue()

        ntopics = 0

        topics_ids = self.__fetch_and_parse_topics_ids(from_date)

        for topic_id in topics_ids:
            topic = self.__fetch_and_parse_topic(topic_id)
            ntopics += 1
            yield topic
            self._flush_cache_queue()

        logger.info("Fetch process completed: %s topics fetched",
                    ntopics)

    @metadata
    def fetch_from_cache(self):
        """Fetch topics from the cache.

        :returns: a generator of topics

        :raises CacheError: raised when an error occurs accessing the
            cache
        """
        if not self.cache:
            raise CacheError(cause="cache instance was not provided")

        logger.info("Retrieving cached topics: '%s'", self.url)

        cache_items = self.cache.retrieve()

        ntopics = 0

        while True:
            try:
                raw_topic = next(cache_items)
            except StopIteration:
                break

            topic = json.loads(raw_topic)

            # Retrieve remaining posts for this topic
            posts_sz = topic['posts_count']
            chunk_sz = topic['chunk_size']

            if posts_sz > chunk_sz:
                for _ in range(posts_sz - chunk_sz):
                    try:
                        raw_post = next(cache_items)
                    except StopIteration:
                        # Fatal error. The code should not reach here.
                        # Cache should had stored posts_sz - chunk_sz posts
                        # if the code is running this loop
                        cause = "cache is exhausted but more items were expected"
                        raise CacheError(cause=cause)

                    post = json.loads(raw_post)
                    topic['post_stream']['posts'].append(post)

            ntopics += 1
            yield topic

        logger.info("Retrieval process completed: %s topics retrieved from cache",
                    ntopics)

    def __fetch_and_parse_topics_ids(self, from_date):
        logger.debug("Fetching and parsing topics ids from %s",
                     str(from_date))

        candidates = []
        page = 0
        fetching = True

        while fetching:
            response = self.client.topics_page(page)
            topics = self.__parse_topics_page(response)

            if not topics:
                fetching = False

            # Topics are sorted by updated date from the newest
            # to the oldest. When a date is older than 'from_date'
            # we have reached to the end. Pinned topics are
            # ignored but added to the list if the date is in range.
            for topic in topics:
                # Pinned
                if topic[2] and topic[1] < from_date:
                    continue
                elif topic[1] < from_date:
                    fetching = False
                    break
                else:
                    candidates.append(topic)

            page += 1

        # Sort topics by date and in reverse order to fetch them from
        # the oldest to the newest
        candidates = sorted(candidates, key=lambda x: x[1])
        topics_ids = [topic[0] for topic in candidates]

        return topics_ids

    def __fetch_and_parse_topic(self, topic_id):
        logger.debug("Fetching and parsing topic %s", topic_id)

        raw_topic = self.client.topic(topic_id)
        self._push_cache_queue(raw_topic)

        topic = json.loads(raw_topic)

        # There are posts that could not included in the topic.
        # When post_count is greater than chunk_size, we have
        # to fetch the remaining posts
        posts_sz = topic['posts_count']
        chunk_sz = topic['chunk_size']

        if posts_sz > chunk_sz:
            posts_ids = topic['post_stream']['stream']
            posts_ids = posts_ids[chunk_sz:]

            for post_id in posts_ids:
                logger.debug("Fetching and parsing post %s", post_id)
                post = self.__fetch_and_parse_post(post_id)
                topic['post_stream']['posts'].append(post)

        return topic

    def __fetch_and_parse_post(self, post_id):
        logger.debug("Fetching and parsing post %s", post_id)
        raw_post = self.client.post(post_id)
        self._push_cache_queue(raw_post)
        post = json.loads(raw_post)
        return post

    def __parse_topics_page(self, raw_json):
        """Parse a topics page stream.

        The result of parsing process is a generator of tuples. Each
        tuple contains de identifier of the topic, the last date
        when it was updated and whether is pinned or not.

        :param raw_json: JSON stream to parse

        :returns: a generator of parsed bugs
        """
        topics_page = json.loads(raw_json)

        topics_ids = []

        for topic in topics_page['topic_list']['topics']:
            topic_id = topic['id']
            updated_at = str_to_datetime(topic['last_posted_at'])
            pinned = topic['pinned']
            topics_ids.append((topic_id, updated_at, pinned))

        return topics_ids

    @classmethod
    def has_caching(cls):
        """Returns whether it supports caching items on the fetch process.

        :returns: this backend supports items cache
        """
        return True

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend supports items resuming
        """
        return True

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Discourse item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Discourse item.

        The timestamp used is extracted from 'last_posted_at' field.
        This date is converted to UNIX timestamp format taking into
        account the timezone of the date.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        ts = item['last_posted_at']
        ts = str_to_datetime(ts)

        return ts.timestamp()

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Discourse item.

        This backend only generates one type of item which is
        'topic'.
        """
        return 'topic'


class DiscourseClient:
    """Discourse API client.

    This class implements a simple client to retrieve topics from
    any Discourse board.

    :param url: URL of the Discourse site
    :param api_key: Discourse API access token

    :raises HTTPError: when an error occurs doing the request
    """
    # Static resources
    ALL_TOPICS = None # Topics do not need a resource
    TOPICS_SUMMARY = 'latest'
    TOPIC = 't'
    POSTS = 'posts'

    # Params
    PKEY = 'api_key'
    PPAGE = 'page'

    # Data type
    TJSON = '.json'

    def __init__(self, url, api_key=None):
        self.url = url
        self.api_key = api_key

    def topics_page(self, page=None):
        """Retrieve the #page summaries of the latest topics.

        :param page: number of page to retrieve
        """
        params = {
            self.PKEY  : self.api_key,
            self.PPAGE : page
        }

        # http://example.com/latest.json
        response = self._call(self.ALL_TOPICS, self.TOPICS_SUMMARY,
                              params=params)

        return response

    def topic(self, topic_id):
        """Retrive the topic with `topic_id` identifier.

        :param topic_id: identifier of the topic to retrieve
        """
        params = {
            self.PKEY  : self.api_key
        }

        # http://example.com/t/8.json
        response = self._call(self.TOPIC, topic_id,
                              params=params)

        return response

    def post(self, post_id):
        """Retrieve the post whit `post_id` identifier.

        :param post_id: identifier of the post to retrieve
        """
        params = {
            self.PKEY  : self.api_key
        }

        # http://example.com/posts/10.json
        response = self._call(self.POSTS, post_id,
                              params=params)

        return response

    def _call(self, res, res_id, params):
        """Run an API command.

        :param res: type of resource to fetch
        :param res_id: identifier of the resource
        :param params: dict with the HTTP parameters needed to run
            the given command
        """
        if res:
            url = urljoin(self.url, res, res_id)
        else:
            url = urljoin(self.url, res_id)
        url += self.TJSON

        logger.debug("Discourse client calls resource: %s %s params: %s",
                     res, res_id, str(params))

        r = requests.get(url, params=params)
        r.raise_for_status()

        return r.text


class DiscourseCommand(BackendCommand):
    """Class to run Discourse backend from the command line."""

    def __init__(self, *args):
        super().__init__(*args)
        self.url = self.parsed_args.url
        self.backend_token = self.parsed_args.backend_token
        self.outfile = self.parsed_args.outfile
        self.tag = self.parsed_args.tag
        self.from_date = str_to_datetime(self.parsed_args.from_date)
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

        self.backend = Discourse(self.url, self.backend_token,
                                 tag=self.tag, cache=cache)

    def CVSformatOutput(self, commits):
        try:
            if self.outfile.name == '<stdout>':
                fileCVS = csv.writer( sys.stdout)
            else:
                fileCVS = csv.writer(open(self.outfile.name, "w+"))
            fileCVS.writerow(["Backend_name", "Backend_version", "Category",
                              "Archetype", "Archived", "bookmarked", "category_id",
                              "Chunk_size", "Closed", "Created_at", "Deleted_at",
                              "Deleted_by", "Archetyype_ST", "Archived_ST", "Bookmarket_ST",
                              "Bumped_ST", "Bumped_at_ST", "Category_id_ST", "Closed_ST",
                              "Created_at_ST", "Fancy_title_ST", "featured_link_ST",
                              "Highest_post_number_ST", "id_ST", "Image_URL_ST",
                              "last_posted_at_ST", "like_count_ST", "liked_ST",
                              "pinned_ST", "Posts_count_ST", "Replay_count_ST",
                              "Slug_ST", "Title_ST", "Unpinned_ST", "Unseen_ST",
                              "Views_ST", "Visible_ST", "Draft", "Draft_key",
                              "Draft_sequence", "Fancy_title", "Featured_link",
                              "Has_summary", "Highest_post_number", "ID",
                              "Last_posted_at", "Like_count", "Participant_count",
                              "Title", "Unpinned", "User_id", "Views", "Visible",
                              "Word_count",
                              "Origin", "Perceval_version", "Tag",
                              "Timestamp", "Updated_on", "Uuid"])
            for commit in commits:
                string = json.dumps(commit, indent=4, sort_keys=True)
                obj = json.loads(string)

                index = 0
                total = len(obj["data"]["details"]["suggested_topics"])
                str_total = str(total)

                while index < len(obj["data"]["details"]["suggested_topics"]):
                    str_index = str(index + 1)
                    relation = str_index + "/" + str_total

                    #try:
                    #    action = obj["data"]["files"][index]["action"]
                    #except Exception:
                    #    action = "-"

                    fileCVS.writerow([obj["backend_name"],
                                      obj["backend_version"],
                                      obj["category"],
                                      obj["data"]["archetype"],
                                      obj["data"]["archived"],
                                      obj["data"]["bookmarked"],
                                      obj["data"]["category_id"],
                                      obj["data"]["chunk_size"],
                                      obj["data"]["closed"],
                                      obj["data"]["created_at"],
                                      obj["data"]["deleted_at"],
                                      obj["data"]["deleted_by"],
                                      relation,
                                      obj["data"]["details"]["suggested_topics"][index]["archetype"],
                                      obj["data"]["details"]["suggested_topics"][index]["archived"],
                                      obj["data"]["details"]["suggested_topics"][index]["bookmarked"],
                                      obj["data"]["details"]["suggested_topics"][index]["bumped"],
                                      obj["data"]["details"]["suggested_topics"][index]["bumped_at"],
                                      obj["data"]["details"]["suggested_topics"][index]["category_id"],
                                      obj["data"]["details"]["suggested_topics"][index]["closed"],
                                      obj["data"]["details"]["suggested_topics"][index]["created_at"],
                                      obj["data"]["details"]["suggested_topics"][index]["fancy_title"],
                                      obj["data"]["details"]["suggested_topics"][index]["featured_link"],
                                      obj["data"]["details"]["suggested_topics"][index]["highest_post_number"],
                                      obj["data"]["details"]["suggested_topics"][index]["id"],
                                      obj["data"]["details"]["suggested_topics"][index]["image_url"],
                                      obj["data"]["details"]["suggested_topics"][index]["last_posted_at"],
                                      obj["data"]["details"]["suggested_topics"][index]["like_count"],
                                      obj["data"]["details"]["suggested_topics"][index]["liked"],
                                      obj["data"]["details"]["suggested_topics"][index]["pinned"],
                                      obj["data"]["details"]["suggested_topics"][index]["posts_count"],
                                      obj["data"]["details"]["suggested_topics"][index]["reply_count"],
                                      obj["data"]["details"]["suggested_topics"][index]["slug"],
                                      obj["data"]["details"]["suggested_topics"][index]["title"],
                                      obj["data"]["details"]["suggested_topics"][index]["unpinned"],
                                      obj["data"]["details"]["suggested_topics"][index]["unseen"],
                                      obj["data"]["details"]["suggested_topics"][index]["views"],
                                      obj["data"]["details"]["suggested_topics"][index]["visible"],
                                      obj["data"]["draft"],
                                      obj["data"]["draft_key"],
                                      obj["data"]["draft_sequence"],
                                      obj["data"]["fancy_title"],
                                      obj["data"]["featured_link"],
                                      obj["data"]["has_summary"],
                                      obj["data"]["highest_post_number"],
                                      obj["data"]["id"],
                                      obj["data"]["last_posted_at"],
                                      obj["data"]["like_count"],
                                      obj["data"]["participant_count"],
                                      obj["data"]["title"],
                                      obj["data"]["unpinned"],
                                      obj["data"]["user_id"],
                                      obj["data"]["views"],
                                      obj["data"]["visible"],
                                      obj["data"]["word_count"],
                                      obj["origin"],
                                      obj["perceval_version"],
                                      obj["tag"],
                                      obj["timestamp"],
                                      obj["updated_on"],
                                      obj["uuid"]])
                    index = index + 1
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
        """Fetch and print the posts.

        This method runs the backend to fetch the topics of a given
        Discourse URL. Topics are converted to JSON objects and printed
        to the defined output.
        """
        if self.parsed_args.fetch_cache:
            topics = self.backend.fetch_from_cache()
        else:
            topics = self.backend.fetch(from_date=self.from_date)

        if self.isCsv:
            self.CVSformatOutput( topics )
        else:
            self.JSONformatOutput( topics )

    @classmethod
    def create_argument_parser(cls):
        """Returns the Discourse argument parser."""

        parser = super().create_argument_parser()

        # Required arguments
        parser.add_argument('url',
                            help="URL of the Discourse server")

        return parser
