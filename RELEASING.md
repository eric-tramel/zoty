# Releasing zoty

This repository publishes two things from the same versioned release:

- The Python package on PyPI
- The Zotero bridge plugin as `zoty-bridge.xpi`

The Zotero bridge also publishes `zoty-bridge-updates.json`, which is the update feed referenced by the XPI manifest. Zotero uses that JSON file to discover future bridge updates and compatibility changes.

## Versioning

Keep the project version in `pyproject.toml` and the bridge version in `zotero-plugin/manifest.json` in sync. `make build` enforces this.

Bump the version whenever bridge code, bridge compatibility, or Python package behavior changes. The bridge version must always increase when the XPI changes, because Zotero will not treat a same-version XPI as an upgrade.

Use a tag that matches the version:

```bash
v0.2.2
```

## Build Artifacts

Build the plugin artifacts with:

```bash
make build
```

This writes:

- `zotero-plugin/dist/zoty-bridge.xpi`
- `zotero-plugin/dist/zoty-bridge-updates.json`

The update manifest includes:

- the bridge version
- a versioned GitHub release URL for `zoty-bridge.xpi`
- the XPI sha256 digest
- Zotero compatibility bounds from `zotero-plugin/manifest.json`

Before committing a release prep change, run:

```bash
make verify-build
make test
```

CI also runs `make build` and fails if the committed XPI or update manifest is stale.

## GitHub Release

Create and push a tag that matches the version in `pyproject.toml` and `zotero-plugin/manifest.json`:

```bash
git tag v0.2.2
git push origin v0.2.2
```

The release workflow:

1. Builds the deterministic Zotero bridge artifacts.
2. Verifies the tag matches the bridge version.
3. Runs unit tests.
4. Builds the Python wheel and sdist.
5. Uploads `zoty-bridge.xpi` and `zoty-bridge-updates.json` to the GitHub release.
6. Publishes the Python package to PyPI.

To test the tag guard locally before pushing a release tag:

```bash
make release-build RELEASE_TAG=v0.2.2
```

After the workflow completes, verify the release assets:

```bash
curl -L https://github.com/eric-tramel/zoty/releases/latest/download/zoty-bridge-updates.json
curl -L https://github.com/eric-tramel/zoty/releases/latest/download/zoty-bridge.xpi -o /tmp/zoty-bridge.xpi
shasum -a 256 /tmp/zoty-bridge.xpi
```

The hash should match the `update_hash` field in the update manifest.

## Zotero Compatibility Updates

Zotero supports updating compatibility through the update manifest. If a future Zotero version only needs a compatibility bump, update `strict_max_version`, bump the bridge version, rebuild, and release the XPI/update manifest pair.

Existing users with an old bridge whose `update_url` points to a missing feed may need one manual reinstall from the latest GitHub release. Once they install a bridge with the current update URL, future updates can flow through Zotero's plugin update mechanism.
