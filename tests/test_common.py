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
from unittest import mock
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
        with mock.patch.dict(os.environ, {"SALT": "test_salt_123"}):
            salt = get_salt(dbutils=None)
            self.assertEqual(salt, "test_salt_123")

    def test_get_salt_no_salt_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                get_salt(dbutils=None)

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
        self.assertEqual(ctx["status"], "SUCCESS")

    def test_build_log_row_valid_statuses(self):
        from src.common import build_log_row
        now = datetime.now(timezone.utc)
        
        # Test SUCCESS with message and GOLD layer
        row_succ = build_log_row(
            log_id="log-1",
            layer="GOLD",
            parameter="table=fact_commits;mode=incremental",
            batch_id="b-1",
            start_time=now,
            end_time=now,
            status="SUCCESS",
            rows_inserted=10,
            rows_updated=5,
            rows_aggregated=100,
            error_message="PARTIAL_QUARANTINE: 2/12 records quarantined",
        )
        self.assertEqual(row_succ[0], "log-1")
        self.assertEqual(row_succ[1], "GOLD")
        self.assertEqual(row_succ[6], "SUCCESS")
        self.assertEqual(row_succ[7], 10)
        self.assertEqual(row_succ[8], 5)
        self.assertEqual(row_succ[9], 100)
        self.assertEqual(row_succ[10], "PARTIAL_QUARANTINE: 2/12 records quarantined")

        # Test FAILURE with message
        row_fail = build_log_row(
            log_id="log-2",
            layer="SILVER",
            parameter="test_p2",
            batch_id="b-2",
            start_time=now,
            end_time=now,
            status="FAILURE",
            rows_inserted=0,
            rows_updated=0,
            error_message="ALL_RECORDS_QUARANTINED",
        )
        self.assertEqual(row_fail[6], "FAILURE")
        self.assertIsNone(row_fail[9])
        self.assertEqual(row_fail[10], "ALL_RECORDS_QUARANTINED")

    def test_build_log_row_invalid_status_raises(self):
        from src.common import build_log_row
        now = datetime.now(timezone.utc)
        for invalid_status in ["EMPTY", "ALL_QUARANTINED", "RUNNING", "PARTIAL", ""]:
            with self.subTest(status=invalid_status):
                with self.assertRaises(ValueError):
                    build_log_row(
                        log_id="log-x",
                        layer="BRONZE",
                        parameter="p",
                        batch_id="b",
                        start_time=now,
                        end_time=now,
                        status=invalid_status,
                    )

    def test_log_run_preserves_message_on_success(self):
        from src.common import log_run
        mock_spark = mock.MagicMock()
        with log_run(mock_spark, layer="GOLD", parameter="p", batch_id="b") as ctx:
            ctx["message"] = "NO_DATA"
            ctx["rows_inserted"] = 0
            ctx["rows_updated"] = 0
            ctx["rows_aggregated"] = 50

        self.assertTrue(mock_spark.createDataFrame.called)
        logged_row = mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(logged_row[1], "GOLD")
        self.assertEqual(logged_row[6], "SUCCESS")
        self.assertEqual(logged_row[9], 50)
        self.assertEqual(logged_row[10], "NO_DATA")


class TestGoldNotebookSignatures(unittest.TestCase):
    """Smoke-test that every helper used by 05_silver_to_gold.py accepts the
    exact argument forms the notebook passes.  A signature change in common.py
    that would blow up on Databricks will fail here first."""

    def _make_mock_spark(self):
        """Return a MagicMock that satisfies merge_delta's spark.catalog.tableExists check."""
        mock_spark = mock.MagicMock()
        # tableExists returns False → merge_delta takes the write-new-table path
        mock_spark.catalog.tableExists.return_value = False
        # df.write chain: write.format().mode().option().saveAsTable()
        mock_spark.createDataFrame.return_value = mock.MagicMock()
        return mock_spark

    def test_start_run_gold_signature(self):
        """start_run(spark, layer, parameter, batch_id, catalog) — as called by the Gold notebook."""
        from src.common import start_run
        ctx = start_run(
            spark=None,        # spark object (None acceptable locally)
            layer="GOLD",
            parameter="table=dim_repo,repo=ALL,entity=repo_metadata",
            batch_id="gold_test_batch",
            catalog="workspace",
        )
        self.assertEqual(ctx["layer"], "GOLD")
        self.assertEqual(ctx["parameter"], "table=dim_repo,repo=ALL,entity=repo_metadata")
        self.assertEqual(ctx["batch_id"], "gold_test_batch")
        self.assertEqual(ctx["catalog"], "workspace")
        self.assertIn("log_id", ctx)
        self.assertIn("start_time", ctx)

    def test_log_run_context_manager_gold_signature(self):
        """log_run(spark, layer, parameter, batch_id, catalog) used as `with` — Gold notebook pattern."""
        from src.common import log_run
        mock_spark = self._make_mock_spark()

        with log_run(
            mock_spark,
            layer="GOLD",
            parameter="table=fact_commits,repo=ALL,entity=commits",
            batch_id="gold_test_batch",
            catalog="workspace",
        ) as run_ctx:
            run_ctx["rows_inserted"] = 42
            run_ctx["rows_updated"] = 7

        # createDataFrame was called → log row was built successfully
        self.assertTrue(mock_spark.createDataFrame.called)
        logged_row = mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(logged_row[1], "GOLD")   # layer
        self.assertEqual(logged_row[6], "SUCCESS") # status
        self.assertEqual(logged_row[7], 42)        # rows_inserted
        self.assertEqual(logged_row[8], 7)         # rows_updated

    def test_log_run_context_manager_rows_aggregated(self):
        """rows_aggregated field used by fact_repo_daily_activity block."""
        from src.common import log_run
        mock_spark = self._make_mock_spark()

        with log_run(
            mock_spark,
            layer="GOLD",
            parameter="table=fact_repo_daily_activity,repo=ALL,entity=daily_activity",
            batch_id="gold_test_batch",
            catalog="workspace",
        ) as run_ctx:
            run_ctx["rows_inserted"] = 100
            run_ctx["rows_updated"] = 50
            run_ctx["rows_aggregated"] = 150   # ins + upd, as set in notebook 05

        logged_row = mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(logged_row[9], 150)   # rows_aggregated
        self.assertEqual(logged_row[6], "SUCCESS")

    def test_log_run_context_manager_failure_path(self):
        """log_run context manager re-raises and logs FAILURE when body raises."""
        from src.common import log_run
        mock_spark = self._make_mock_spark()

        with self.assertRaises(RuntimeError):
            with log_run(
                mock_spark,
                layer="GOLD",
                parameter="table=dim_actor,repo=ALL,entity=actors",
                batch_id="gold_test_batch",
                catalog="workspace",
            ) as run_ctx:
                run_ctx["rows_inserted"] = 0
                raise RuntimeError("simulated transform failure")

        logged_row = mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(logged_row[6], "FAILURE")
        self.assertIn("simulated transform failure", logged_row[10])

    def test_merge_delta_signature_df_before_target_table(self):
        """merge_delta(spark, df, target_table, keys, update_all) — df comes before target_table."""
        from src.common import merge_delta
        mock_spark = self._make_mock_spark()

        # Build a mock DataFrame with a write chain
        mock_df = mock.MagicMock()
        mock_df.count.return_value = 5
        write_chain = mock.MagicMock()
        mock_df.write = write_chain
        write_chain.format.return_value = write_chain
        write_chain.mode.return_value = write_chain
        write_chain.option.return_value = write_chain

        ins, upd = merge_delta(
            mock_spark,          # spark
            mock_df,             # df  ← must be 2nd positional arg
            "workspace.gold.dim_repo",  # target_table ← 3rd positional arg
            ["repo_key"],        # keys
            update_all=True,
        )
        # tableExists returned False → write-new-table path → inserted == df.count()
        self.assertEqual(ins, 5)
        self.assertEqual(upd, 0)


if __name__ == "__main__":
    unittest.main()
