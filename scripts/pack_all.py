# -*- coding: utf-8 -*-
"""交叉编译产物 -> 五平台 .dbxp -> 复用 dbx-ads-arm 的 trusted key 签名。

前置：先运行 scripts/build_cross.sh 生成 bin/cross/dbx-plugin-flink-studio-<target>。
签名 key 复用 C:/asdm-tools/dbx-ads-arm/.signing（193 dbx 已信任该 key）。

用法：python scripts/pack_all.py [--target linux-x64] [--no-sign]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DIST = HERE / "dist"
CROSS = HERE / "bin" / "cross"
SIGN_SCRIPT = Path("C:/asdm-tools/dbx-ads-arm/sign.py")
TARGETS = ["linux-x64", "linux-arm64", "darwin-x64", "darwin-arm64", "windows-x64"]
BINARY = "dbx-plugin-flink-studio"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def pack(target: str) -> Path:
    manifest_path = HERE / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pid, version = manifest["id"], manifest["version"]

    src_bin = CROSS / (BINARY + "-" + target)
    if not src_bin.exists():
        raise SystemExit("缺少二进制：%s" % src_bin)
    if target.startswith("windows"):
        bin_name = BINARY + ".exe"
    else:
        bin_name = BINARY
    # DBX installer resolves the sidecar by the manifest executable path.
    # ads-arm (verified on 193) uses "bin/<target>/<binary>" — mirror it.
    exe_rel = f"bin/{target}/{bin_name}"
    manifest["entrypoints"]["backend"]["executable"] = exe_rel

    # Stage files in a temp dir so the package uses the official layout
    stage = DIST / (".stage-" + target)
    if stage.exists():
        shutil.rmtree(stage)
    (stage / "bin" / target).mkdir(parents=True)
    (stage / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    shutil.copy2(src_bin, stage / "bin" / target / bin_name)
    for d in ("assets", "ui"):
        src = HERE / d
        if src.exists():
            shutil.copytree(src, stage / d)

    files = sorted(p for p in stage.rglob("*") if p.is_file())
    checksums = {}
    for f in files:
        checksums[f.relative_to(stage).as_posix()] = sha256(f)
    blob = json.dumps({"algorithm": "sha256", "files": checksums},
                      ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

    DIST.mkdir(parents=True, exist_ok=True)
    out = DIST / f"{pid}-{version}-{target}.dbxp"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for f in files:
            rel = f.relative_to(stage).as_posix()
            info = zipfile.ZipInfo(rel, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = 0o755 if rel.startswith("bin/") else 0o644
            info.external_attr = (0o100000 | mode) << 16
            zf.writestr(info, f.read_bytes())
        info = zipfile.ZipInfo("checksums.json", date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (0o100000 | 0o644) << 16
        zf.writestr(info, blob)
    shutil.rmtree(stage)
    print("packed  %s  (%.2f MB)" % (out.name, out.stat().st_size / 1048576.0))
    return out


def sign(pkg: Path) -> Path:
    if not SIGN_SCRIPT.exists():
        raise SystemExit("签名脚本不存在：" + str(SIGN_SCRIPT))
    out = pkg.with_name(pkg.stem + ".signed.dbxp")
    if out.exists():
        out.unlink()
    r = subprocess.run([sys.executable, str(SIGN_SCRIPT), str(pkg), "--out", str(out)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit("签名失败：" + r.stdout + r.stderr)
    print("signed  %s" % out.name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=TARGETS)
    ap.add_argument("--no-sign", action="store_true")
    args = ap.parse_args()
    targets = [args.target] if args.target else TARGETS
    for t in targets:
        pkg = pack(t)
        if not args.no_sign:
            sign(pkg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
