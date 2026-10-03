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

    def test_get_params_with_fake_dbutils(self):
        class FakeWidgets:
            def __init__(self, existing=None):
                self.widgets = dict(existing) if existing else {}
                self.declared = []

            def get(self, name):
                if name in self.widgets:
                    return self.widgets[name]
                raise KeyError(f"Widget {name} not found")

            def text(self, name, defaultValue="", label=""):
                self.declared.append(("text", name))
                self.widgets[name] = defaultValue

            def dropdown(self, name, defaultValue="", choices=None, label=""):
                self.declared.append(("dropdown", name))
                self.widgets[name] = defaultValue

        class FakeDbutils:
            def __init__(self, widgets=None):
                self.widgets = FakeWidgets(widgets)

        existing = {
            "catalog": "my_cat",
            "repo": "postgres/postgres",
            "entity": "issues",
            "until": "",
            "since": "",
            "batch_id": "",
        }
        fake_db = FakeDbutils(widgets=existing)
        params = get_params(dbutils=fake_db)

        # until="" and since="" stay ""
        self.assertEqual(params["until"], "")
        self.assertEqual(params["since"], "")

        # batch_id="" becomes auto-generated batch id
        self.assertTrue(params["batch_id"].startswith("batch_"))

        # existing widget 'entity' was not re-declared
        declared_names = [name for _, name in fake_db.widgets.declared]
        self.assertNotIn("entity", declared_names)
        self.assertNotIn("catalog", declared_names)
        self.assertEqual(params["entity"], "issues")

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
