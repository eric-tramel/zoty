import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from zoty import cli


class CliMainTests(unittest.TestCase):
    def test_version_prints_package_version_without_starting_mcp(self):
        stdout = io.StringIO()
        with (
            patch.object(cli.setup, "package_version", return_value="1.2.3"),
            patch.object(cli, "_run_mcp") as mcp_mock,
            redirect_stdout(stdout),
        ):
            exit_code = cli.main(["--version"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue(), "zoty 1.2.3\n")
        mcp_mock.assert_not_called()

    def test_short_version_prints_package_version_without_starting_mcp(self):
        stdout = io.StringIO()
        with (
            patch.object(cli.setup, "package_version", return_value="1.2.3"),
            patch.object(cli, "_run_mcp") as mcp_mock,
            redirect_stdout(stdout),
        ):
            exit_code = cli.main(["-V"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue(), "zoty 1.2.3\n")
        mcp_mock.assert_not_called()

    def test_no_args_prints_human_help_without_starting_mcp(self):
        stdout = io.StringIO()
        with (
            patch.object(cli, "_run_mcp", return_value=0) as mcp_mock,
            redirect_stdout(stdout),
        ):
            exit_code = cli.main([])

        self.assertEqual(exit_code, 0)
        self.assertIn("usage: zoty", stdout.getvalue())
        mcp_mock.assert_not_called()

    def test_mcp_subcommand_forwards_server_args(self):
        with patch.object(cli, "_run_mcp", return_value=0) as mcp_mock:
            exit_code = cli.main(
                [
                    "mcp",
                    "--transport",
                    "streamable-http",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8000",
                ]
            )

        self.assertEqual(exit_code, 0)
        mcp_mock.assert_called_once_with(
            [
                "--transport",
                "streamable-http",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
            ]
        )

    def test_setup_check_is_non_mutating(self):
        result = cli.setup.DoctorResult(
            checks=(
                cli.setup.CheckResult("Zotero local API", True, "ok"),
                cli.setup.CheckResult("zoty-bridge", True, "ok, version 1.2.3"),
            ),
            package_version="1.2.3",
            bundled_bridge_version="1.2.3",
            installed_bridge_version="1.2.3",
            latest_bridge_version="1.2.3",
        )
        stdout = io.StringIO()
        with (
            patch.object(cli.setup, "resolve_setup_xpi"),
            patch.object(cli.setup, "run_doctor", return_value=result),
            patch.object(cli.setup, "install_bridge_into_profile") as install_mock,
            redirect_stdout(stdout),
        ):
            exit_code = cli.main(["setup", "--check"])

        self.assertEqual(exit_code, 0)
        self.assertIn("Status: ready", stdout.getvalue())
        install_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
