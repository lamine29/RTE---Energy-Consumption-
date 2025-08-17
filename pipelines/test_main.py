import os
import unittest
from unittest.mock import patch
import requests
from pipelines.main import get_access_token

class TestGetAccessToken(unittest.TestCase):
    @patch('requests.post')
    def test_successful_token(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'access_token': 'test_token'}
        token = get_access_token()
        self.assertEqual(token, 'test_token')

    @patch('requests.post')
    def test_failed_token(self, mock_post):
        mock_post.return_value.status_code = 400
        mock_post.return_value.json.return_value = {}
        with self.assertRaises(KeyError):
            get_access_token()

if __name__ == '__main__':
    unittest.main()
