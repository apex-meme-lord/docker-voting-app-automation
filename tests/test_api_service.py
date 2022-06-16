import pytest


class TestAPIBasicAcceptance():

    def test_vote_for_a(self, api, db, voter_id):
        api.post_vote('a')
        assert db.fetch_vote_by_id(voter_id) == 'a'

    def test_change_vote(self, api, db, voter_id):
        api.post_vote('a')
        api.post_vote('b')
        assert db.fetch_vote_by_id(voter_id) == 'b'


