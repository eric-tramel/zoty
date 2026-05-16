"""Top-level command line interface for zoty."""

from __future__ import annotations

import argparse
import sys

from zoty import setup


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zoty",
        description="Run the zoty MCP server and manage Zotero bridge setup.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Print the installed zoty version and exit.",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("mcp", help="Run the zoty MCP server.")

    doctor_parser = subparsers.add_parser("doctor", help="Run non-mutating setup diagnostics.")
    doctor_parser.add_argument(
        "--no-network",
        action="store_true",
        help="Skip checks that call GitHub release metadata.",
    )

    setup_parser = subparsers.add_parser("setup", help="Guide Zotero bridge setup or upgrade.")
    setup_parser.add_argument(
        "--check",
        action="store_true",
        help="Run setup diagnostics without making changes.",
    )
    setup_parser.add_argument(
        "--download-only",
        action="store_true",
        help="Print the XPI path to use and do not install.",
    )
    setup_parser.add_argument(
        "--force",
        action="store_true",
        help="Show install guidance even when the bridge appears current.",
    )
    setup_parser.add_argument(
        "--install-profile",
        action="store_true",
        help="Copy the XPI into the default Zotero profile. Zotero must be closed.",
    )
    setup_parser.add_argument(
        "--xpi",
        help="Use a local zoty-bridge.xpi instead of the bundled artifact.",
    )
    setup_parser.add_argument(
        "--no-network",
        action="store_true",
        help="Only use bundled artifacts and local checks.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)

    if args_list and args_list[0] == "mcp":
        return _run_mcp(args_list[1:])

    parser = build_parser()
    args = parser.parse_args(args_list)

    if args.version:
        print(f"zoty {setup.package_version()}")
        return 0

    if args.command == "doctor":
        return _run_doctor(no_network=args.no_network)

    if args.command == "setup":
        return _run_setup(args)

    parser.print_help()
    return 0


def _run_mcp(argv: list[str]) -> int:
    from zoty import server

    server.main(argv)
    return 0


def _run_doctor(no_network: bool = False) -> int:
    result = setup.run_doctor(no_network=no_network)
    print(setup.format_doctor(result))
    return 0 if result.ready else 1


def _run_setup(args: argparse.Namespace) -> int:
    try:
        xpi_path = setup.resolve_setup_xpi(args.xpi)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.download_only:
        print(xpi_path)
        return 0

    result = setup.run_doctor(no_network=args.no_network)
    if args.check:
        print(setup.format_doctor(result))
        return 0 if result.ready else 1

    if args.install_profile:
        try:
            destination = setup.install_bridge_into_profile(xpi_path)
        except (FileNotFoundError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print(f"Installed zoty-bridge to {destination}")
        print("Restart Zotero, then run `zoty doctor` to verify the bridge.")
        return 0

    print(setup.setup_guidance(xpi_path, result, force=args.force))
    return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
