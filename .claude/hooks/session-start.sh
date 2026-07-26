#!/bin/bash
# SessionStart hook for Claude Code on the web: sync deps and seed DuckDB's
# extension cache from PyPI wheels, so `INSTALL httpfs` never needs network
# access to extensions.duckdb.org (blocked by default egress policies).
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"
uv sync

uv run --with pip python - <<'PY'
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import duckdb

ver = duckdb.__version__
plat = duckdb.execute("PRAGMA platform").fetchone()[0]
dest = Path.home() / ".duckdb" / "extensions" / f"v{ver}" / plat
missing = [e for e in ("httpfs", "aws", "ducklake") if not (dest / f"{e}.duckdb_extension").exists()]
if not missing:
    print(f"duckdb extensions already cached in {dest}")
    sys.exit(0)

# Each extension is published on PyPI as duckdb-extension-<name>==<duckdb version>.
with tempfile.TemporaryDirectory() as tmp:
    subprocess.run(
        [sys.executable, "-m", "pip", "download", "--no-deps", "-q", "-d", tmp]
        + [f"duckdb-extension-{e}=={ver}" for e in missing],
        check=True,
    )
    dest.mkdir(parents=True, exist_ok=True)
    for whl in Path(tmp).glob("*.whl"):
        with zipfile.ZipFile(whl) as z:
            for name in z.namelist():
                if name.endswith(".duckdb_extension"):
                    (dest / Path(name).name).write_bytes(z.read(name))
print(f"seeded {missing} into {dest}")
PY
