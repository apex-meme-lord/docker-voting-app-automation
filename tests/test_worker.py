import pytest


class TestWorkerBasicAcceptance():

    def test_vote_for_a(self, redis, db, voter_id):
        with db.expect_vote_updated(voter_id):
            redis.push_vote('a', voter_id)
        assert db.fetch_vote_by_id(voter_id) == 'a'

    def test_change_vote(self, redis, db, voter_id):
        with db.expect_vote_updated(voter_id):
            redis.push_vote('a', voter_id)
        with db.expect_vote_updated(voter_id):
            redis.push_vote('b', voter_id)
        assert db.fetch_vote_by_id(voter_id) == 'b'
