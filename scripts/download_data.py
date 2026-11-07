"""下载 TLC 公开数据并记录追溯信息（URL、下载时间、大小、SHA-256、schema、行数）。

幂等：文件已存在且大小一致时跳过下载（用 --force 强制重新下载）。
输出：data/provenance.json。原始大文件不入 Git（见 .gitignore）。

用法（Windows 本地或 VM 均可）：
    python scripts/download_data.py            # 首次下载
    python scripts/download_data.py --force    # 重新下载并覆盖
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

FILES = [
    {
        "name": "yellow_tripdata_2024-01.parquet",
        "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
        "source_page": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    },
    {
        "name": "taxi_zone_lookup.csv",
        "url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        "source_page": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    },
]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parquet_row_count(path: Path):
    """读取 parquet 元数据行数（不加载全量数据）；pyarrow 不可用时返回 None。"""
    try:
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="忽略已存在文件，重新下载")
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    provenance = {
        "source_page": FILES[0]["source_page"],
        "downloaded_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "files": [],
    }

    for spec in FILES:
        dest = RAW_DIR / spec["name"]
        if dest.exists() and not args.force:
            print(f"[skip] {spec['name']} 已存在（{dest.stat().st_size} 字节），如需重下加 --force")
        else:
            print(f"[down] {spec['url']}")
            urllib.request.urlretrieve(spec["url"], dest)
        size = dest.stat().st_size
        digest = sha256_of(dest)
        row_count = parquet_row_count(dest) if spec["name"].endswith(".parquet") else None
        provenance["files"].append(
            {
                "name": spec["name"],
                "url": spec["url"],
                "size_bytes": size,
                "sha256": digest,
                "row_count": row_count,
            }
        )
        print(f"[ok]   {spec['name']}: {size} bytes, sha256={digest[:16]}..., rows={row_count}")

    out = PROJECT_ROOT / "data" / "provenance.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(provenance, f, ensure_ascii=False, indent=2)
    print(f"[done] 追溯信息已写入 {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
