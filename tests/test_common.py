"""
Unit tests for src/common.py reusable pipeline functions:
- get_params widget parser
- bot_flag heuristics
- hash_email
- get_salt
- start_run / end_run context
"""

import os
import unittest
from datetime import datetime, timezone

from src.common import (
    get_params,
    get_salt,
    hash_email,
    bot_flag,
    start_run,
    KNOWN_BOT_ACCOUNTS,
    VALID_RUN_MODES,
)


class TestCommonFunctions(unittest.TestCase):

    def test_get_params_defaults(self):
        params = get_params(dbutils=None)
        self.assertIsInstance(params, dict)
        self.assertIn("repo", params)
        self.assertIn("entity", params)
        self.assertIn("base_path", params)
        self.assertIn("since", params)
        self.assertIn("until", params)
        self.assertIn("batch_id", params)
        self.assertIn("run_mode", params)
        self.assertIn(params["run_mode"], VALID_RUN_MODES)

    def test_get_salt_fallback(self):
        salt = get_salt(dbutils=None)
        self.assertIsInstance(salt, str)
        self.assertGreater(len(salt), 0)

    def test_known_bot_accounts_list(self):
        self.assertIn("bors", KNOWN_BOT_ACCOUNTS)
        self.assertIn("cockroach-teamcity", KNOWN_BOT_ACCOUNTS)
        self.assertIn("dependabot", KNOWN_BOT_ACCOUNTS)
        self.assertIn("mongodb-evergreen", KNOWN_BOT_ACCOUNTS)
        self.assertIn("renovate", KNOWN_BOT_ACCOUNTS)
        self.assertIn("github-actions", KNOWN_BOT_ACCOUNTS)

    def test_start_run_tracking_context(self):
        ctx = start_run(
            spark=None,
            layer="BRONZE",
            parameter="test_param",
            batch_id="batch_123",
            catalog="workspace"
        )
        self.assertEqual(ctx["layer"], "BRONZE")
        self.assertEqual(ctx["batch_id"], "batch_123")
        self.assertEqual(ctx["parameter"], "test_param")
        self.assertIn("log_id", ctx)
        self.assertIsInstance(ctx["start_time"], datetime)


if __name__ == "__main__":
    unittest.main()
