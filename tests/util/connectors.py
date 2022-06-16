import json
import logging
import psycopg2
import redis
import requests
import time
from contextlib import contextmanager


LOG = logging.getLogger()
APP_PORT = 8080
REDIS_PORT = 8081
PSQL_PORT = 8082


class DBConnector():
    def __init__(self, host='localhost', port=PSQL_PORT, db='postgres', user='postgres'):
        self.connect = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user
        )

    def fetch_all_votes(self):
        """
        Fetches all rows from the votes table and returns a tuple
        of tuples in the format ((id1, vote1), (id2, vote2), ...)
        """
        with self.connect.cursor() as curs:
            curs.execute(
                """
                SELECT * FROM votes;
                """
            )
            return curs.fetchall()

    def fetch_vote_by_id(self, voter_id):
        """
        Fetches the vote value of the specified `voter_id`.  Raises
        exception on multiple matches.  Returns None for non-match.
        """
        with self.connect.cursor() as curs:
            curs.execute(
                """
                SELECT vote from votes where id = %(id)s;
                """,
                {'id': voter_id}
            )
            results = curs.fetchall()
            
            if len(results) == 0:
                return None
            assert len(results) == 1, "Votes for id {} not equal to 1: {}".format(voter_id, results)
            return results[0][0]

    @contextmanager
    def expect_vote_updated(self, voter_id, timeout=60):
        """
        Context manager that waits 60 seconds for the db to update
        before return.
        """
        old = self.fetch_vote_by_id(voter_id)

        try:
            yield self
        finally:
            timeout = time.time() + 60
            while time.time() < timeout:
                new = self.fetch_vote_by_id(voter_id)
                if old != new:
                    return
                LOG.warning("Expected vote value not updated")
                time.sleep(1)


class RedisConnector():
    def __init__(self, host='localhost', port=REDIS_PORT):
        self.client = redis.Redis(
            host=host,
            port=port
        )

    def push_vote(self, vote, voter_id):
        """
        Pushes a vote in the back of the queue
        """
        ret = self.client.rpush(
            'votes',
            json.dumps({
                "vote": vote,
                "voter_id": voter_id
            })
        )
        assert ret == 1

    def insert_vote(self, vote, voter_id):
        """
        Inserts a vote in the front of the queue
        """
        ret = self.client.lpush(
            'votes',
            json.dumps({
                "vote": vote,
                "voter_id": voter_id
            })
        )
        assert ret == 1


class APIConnector():
    def __init__(self, host='localhost', port=APP_PORT):
        self.host = host
        self.port = port
        self.session = requests.Session()

    @property
    def voter_id(self):
        """
        Returns the voter_id from the session cookie.  Opens a
        new session if not yet opened
        """
        if 'voter_id' not in self.session.cookies:
            LOG.warning("No voter_id cookie")
            self._init_cookies()
        return self.session.cookies.get('voter_id')

    @property
    def url(self):
        """
        Returns the formated url
        """
        return "http://{}:{}/".format(self.host, self.port)

    def _init_cookies(self):
        """
        GET the web service for initializing the cookie
        """
        LOG.debug("Getting voter_id cookie")
        self.session.get(self.url)

    def _post(self, data, *args, **kwargs):
        """
        Wrapper session.post.  Raises exception on HTTP
        error codes.
        """
        resp = self.session.post(
            self.url,
            data,
            *args,
            **kwargs
        )
        resp.raise_for_status()
        return resp

    def post_vote(self, vote, voter_id=None):
        """
        POST a vote to the web service
        """
        if voter_id:
            return self._post(
                data={'vote': vote},
                cookies={'voter_id', voter_id}
            )
        return self._post(
            data={'vote': vote}
        )

