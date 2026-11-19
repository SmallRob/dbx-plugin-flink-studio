#!/usr/bin/env python3
"""Validate manifest.json consistency (identity, version, paths, SDK wiring)."""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
errors = []


def check(cond, msg):
    if not cond:
        errors.append(msg)


manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
main_go = (ROOT / "backend" / "main.go").read_text(encoding="utf-8")
toml = (ROOT / "dbx-plugin.toml").read_text(encoding="utf-8")

check(manifest.get("id") == "local.flink.studio", "manifest.id must be local.flink.studio")
check(manifest.get("manifest_version") == 1, "manifest_version must be 1")

m = re.search(r'pluginVersion\s*=\s*"([^"]+)"', main_go)
check(m is not None and m.group(1) == manifest.get("version"),
      "backend/main.go pluginVersion must equal manifest.version")

check('"executable": "bin/dbx-plugin-flink-studio"' in json.dumps(manifest),
      "entrypoints.backend.executable must be bin/dbx-plugin-flink-studio")
check('binary = "dbx-plugin-flink-studio"' in toml, "dbx-plugin.toml binary mismatch")
check(manifest["entrypoints"]["ui"]["entry"] == "ui/index.html", "ui entry must be ui/index.html")
check((ROOT / "ui" / "index.html").exists(), "ui/index.html is missing")
check((ROOT / "assets" / "plugin.svg").exists(), "assets/plugin.svg is missing")

contrib = manifest.get("contributions", [])
types = [c.get("type") for c in contrib]
for required in ("connection-provider", "workbench", "command", "menus"):
    check(required in types, f"missing contribution: {required}")

cp = next((c for c in contrib if c.get("type") == "connection-provider"), {})
check(cp.get("database_type") == "flink", "connection-provider.database_type must be flink")
check(any(f.get("key") == "port" and f.get("default") == 8081 for f in cp.get("fields", [])),
      "connection port default must be 8081")
check(any(f.get("key") == "read_only" for f in cp.get("fields", [])), "read_only field missing")

if errors:
    print("FAIL:")
    for e in errors:
        print("  -", e)
    sys.exit(1)
print(f"OK: {manifest['id']} v{manifest['version']} ({len(contrib)} contributions)")
