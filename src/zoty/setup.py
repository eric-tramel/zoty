"""Human-facing setup and diagnostics for zoty."""

from __future__ import annotations

import configparser
import json
import platform
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from importlib import metadata, resources
from pathlib import Path
from typing import Any

BRIDGE_ADDON_ID = "zoty-bridge@zoty.dev"
BRIDGE_ENDPOINT = "http://127.0.0.1:24119/status"
LATEST_UPDATE_MANIFEST_URL = (
    "https://github.com/eric-tramel/zoty/releases/latest/download/zoty-bridge-updates.json"
)
LOCAL_API_ENDPOINT = "http://127.0.0.1:23119/connector/ping"


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    action: str = ""


@dataclass(frozen=True)
class DoctorResult:
    checks: tuple[CheckResult, ...]
    package_version: str
    bundled_bridge_version: str | None
    installed_bridge_version: str | None
    latest_bridge_version: str | None

    @property
    def ready(self) -> bool:
        required = {"Zotero local API", "zoty-bridge"}
        return all(check.ok for check in self.checks if check.name in required)


def package_version() -> str:
    try:
        return metadata.version("zoty")
    except metadata.PackageNotFoundError:
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        try:
            for line in pyproject.read_text().splitlines():
                if line.startswith("version = "):
                    return line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
        return "0.0.0+unknown"


def bundled_xpi_path() -> Path | None:
    try:
        path = resources.files("zoty.assets").joinpath("zoty-bridge.xpi")
    except ModuleNotFoundError:
        return None

    if not path.is_file():
        return None

    with resources.as_file(path) as concrete_path:
        return Path(concrete_path)


def bridge_version_from_xpi(path: Path) -> str | None:
    try:
        with zipfile.ZipFile(path) as archive:
            with archive.open("manifest.json") as manifest_file:
                manifest = json.load(manifest_file)
    except (FileNotFoundError, KeyError, OSError, json.JSONDecodeError, zipfile.BadZipFile):
        return None

    version = manifest.get("version")
    return str(version) if version else None


def parse_latest_bridge_version(update_manifest: dict[str, Any]) -> str | None:
    updates = (
        update_manifest.get("addons", {})
        .get(BRIDGE_ADDON_ID, {})
        .get("updates", [])
    )
    versions = [str(update["version"]) for update in updates if update.get("version")]
    return max(versions, key=_version_key) if versions else None


def fetch_latest_update_manifest(timeout: float = 2.0) -> dict[str, Any] | None:
    request = urllib.request.Request(
        LATEST_UPDATE_MANIFEST_URL,
        headers={"User-Agent": f"zoty/{package_version()}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None


def zotero_profile_root() -> Path:
    system = platform.system()
    if system == "Darwin":
        return Path.home() / "Library/Application Support/Zotero"
    if system == "Windows":
        return Path.home() / "AppData/Roaming/Zotero/Zotero"
    return Path.home() / ".zotero/zotero"


def discover_default_profile(root: Path | None = None) -> Path | None:
    root = root or zotero_profile_root()
    profiles_ini = root / "profiles.ini"
    parser = configparser.ConfigParser()
    if not parser.read(profiles_ini):
        return None

    fallback: Path | None = None
    for section in parser.sections():
        if not section.startswith("Profile"):
            continue
        raw_path = parser.get(section, "Path", fallback="")
        if not raw_path:
            continue
        is_relative = parser.get(section, "IsRelative", fallback="1") == "1"
        profile_path = root / raw_path if is_relative else Path(raw_path)
        if fallback is None:
            fallback = profile_path
        if parser.get(section, "Default", fallback="0") == "1":
            return profile_path
    return fallback


def is_zotero_running() -> bool:
    if platform.system() == "Windows":
        command = ["tasklist"]
        try:
            output = subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL)
        except (OSError, subprocess.SubprocessError):
            return False
        return "zotero.exe" in output.lower()

    try:
        subprocess.check_output(["pgrep", "-x", "Zotero"], stderr=subprocess.DEVNULL)
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


def http_json(url: str, timeout: float = 1.0) -> tuple[bool, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return False, str(exc)

    if not body:
        return True, ""
    try:
        return True, json.loads(body)
    except json.JSONDecodeError:
        return True, body


def run_doctor(no_network: bool = False) -> DoctorResult:
    version = package_version()
    xpi_path = bundled_xpi_path()
    bundled_version = bridge_version_from_xpi(xpi_path) if xpi_path else None
    latest_version = None
    checks: list[CheckResult] = []

    if xpi_path and bundled_version:
        checks.append(CheckResult("Bundled bridge", True, f"{bundled_version} at {xpi_path}"))
    elif xpi_path:
        checks.append(CheckResult("Bundled bridge", False, f"found at {xpi_path}, but version is unreadable"))
    else:
        checks.append(
            CheckResult(
                "Bundled bridge",
                False,
                "not found in the installed Python package",
                "Reinstall or upgrade zoty; the wheel should include zoty-bridge.xpi.",
            )
        )

    if no_network:
        checks.append(CheckResult("Latest bridge", True, "skipped (--no-network)"))
    else:
        manifest = fetch_latest_update_manifest()
        latest_version = parse_latest_bridge_version(manifest) if manifest else None
        if latest_version:
            checks.append(CheckResult("Latest bridge", True, latest_version))
        else:
            checks.append(CheckResult("Latest bridge", True, "unavailable; continuing with bundled bridge"))

    profile = discover_default_profile()
    if profile:
        checks.append(CheckResult("Zotero profile", True, str(profile)))
    else:
        checks.append(
            CheckResult(
                "Zotero profile",
                False,
                "not found",
                "Start Zotero once so it creates a profile.",
            )
        )

    api_ok, api_payload = http_json(LOCAL_API_ENDPOINT)
    if api_ok:
        checks.append(CheckResult("Zotero local API", True, "ok"))
    else:
        checks.append(
            CheckResult(
                "Zotero local API",
                False,
                f"not reachable at {LOCAL_API_ENDPOINT}: {api_payload}",
                "Start Zotero and enable extensions.zotero.httpServer.localAPI.enabled.",
            )
        )

    bridge_ok, bridge_payload = http_json(BRIDGE_ENDPOINT)
    installed_version = _bridge_payload_version(bridge_payload) if bridge_ok else None
    if bridge_ok:
        detail = f"ok, version {installed_version}" if installed_version else "ok"
        if installed_version and latest_version and compare_versions(installed_version, latest_version) < 0:
            checks.append(
                CheckResult(
                    "zoty-bridge",
                    False,
                    f"{detail}; latest is {latest_version}",
                    "Run `zoty setup` to install or upgrade zoty-bridge.xpi.",
                )
            )
        else:
            checks.append(CheckResult("zoty-bridge", True, detail))
    else:
        checks.append(
            CheckResult(
                "zoty-bridge",
                False,
                f"not reachable at {BRIDGE_ENDPOINT}: {bridge_payload}",
                "Run `zoty setup` or install zoty-bridge.xpi from the latest GitHub release via Zotero Tools -> Plugins.",
            )
        )

    return DoctorResult(
        checks=tuple(checks),
        package_version=version,
        bundled_bridge_version=bundled_version,
        installed_bridge_version=installed_version,
        latest_bridge_version=latest_version,
    )


def format_doctor(result: DoctorResult) -> str:
    lines = [f"zoty {result.package_version}"]
    for check in result.checks:
        marker = "ok" if check.ok else "needs attention"
        lines.append(f"{check.name}: {marker} - {check.detail}")
        if check.action:
            lines.append(f"Next: {check.action}")
    lines.append(f"Status: {'ready' if result.ready else 'setup incomplete'}")
    return "\n".join(lines)


def install_bridge_into_profile(xpi_path: Path, profile_path: Path | None = None) -> Path:
    if is_zotero_running():
        raise RuntimeError("Zotero is running; quit Zotero before using --install-profile.")
    if not xpi_path.is_file():
        raise FileNotFoundError(f"XPI not found: {xpi_path}")

    profile_path = profile_path or discover_default_profile()
    if profile_path is None:
        raise RuntimeError("No Zotero profile found.")

    extensions_dir = profile_path / "extensions"
    extensions_dir.mkdir(parents=True, exist_ok=True)
    destination = extensions_dir / f"{BRIDGE_ADDON_ID}.xpi"
    if destination.exists():
        backup = destination.with_suffix(f".xpi.bak-{int(time.time())}")
        shutil.copy2(destination, backup)
    shutil.copy2(xpi_path, destination)
    return destination


def resolve_setup_xpi(xpi: str | None) -> Path:
    if xpi:
        path = Path(xpi).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"XPI not found: {path}")
        return path
    bundled = bundled_xpi_path()
    if bundled is None:
        raise FileNotFoundError("Bundled zoty-bridge.xpi was not found in this zoty installation.")
    return bundled


def setup_guidance(xpi_path: Path, result: DoctorResult, force: bool = False) -> str:
    if result.ready and not force:
        return format_doctor(result) + "\nzoty-bridge is installed and current."

    lines = [
        format_doctor(result),
        "",
        f"Use this XPI: {xpi_path}",
        "Install path:",
        "1. Open Zotero Tools -> Plugins.",
        "2. Drag zoty-bridge.xpi into the Plugins window.",
        "3. Restart Zotero.",
        "4. Run `zoty doctor` to verify the bridge.",
    ]
    return "\n".join(lines)


def compare_versions(left: str, right: str) -> int:
    left_key = _version_key(left)
    right_key = _version_key(right)
    return (left_key > right_key) - (left_key < right_key)


def _version_key(version: str) -> tuple[int | str, ...]:
    parts: list[int | str] = []
    for part in version.replace("-", ".").split("."):
        if part.isdigit():
            parts.append(int(part))
        elif part:
            parts.append(part)
    return tuple(parts)


def _bridge_payload_version(payload: Any) -> str | None:
    if isinstance(payload, dict):
        version = payload.get("version") or payload.get("bridgeVersion")
        return str(version) if version else None
    return None
