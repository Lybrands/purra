#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source_dir="$(cd "$project_dir/.." && pwd)"
work_dir="$project_dir/.work"
artifacts_dir="$work_dir/artifacts"
results_dir="$work_dir/results"
python_site="$work_dir/python-site"
export npm_config_cache="$work_dir/npm-cache"
export PIP_CACHE_DIR="$work_dir/pip-cache"

if [[ -n "${PYTHON:-}" ]]; then
  python_bin="$PYTHON"
elif [[ -x "$source_dir/.venv/bin/python" ]] \
  && "$source_dir/.venv/bin/python" -m pip --version >/dev/null 2>&1; then
  python_bin="$source_dir/.venv/bin/python"
else
  python_bin="python3"
fi

rm -rf "$work_dir" "$project_dir/dist" "$project_dir/node_modules"
mkdir -p "$artifacts_dir" "$results_dir" "$python_site"

"$python_bin" -c 'import sys; assert sys.version_info >= (3, 11), sys.version'
"$python_bin" -m pip --version >/dev/null
node -e 'const major = Number(process.versions.node.split(".")[0]); if (major < 22) throw new Error(`Node 22+ required; got ${process.version}`)'

"$python_bin" -m pip wheel \
  --disable-pip-version-check \
  --no-deps \
  --no-build-isolation \
  --wheel-dir "$artifacts_dir" \
  "$source_dir"
python_wheel="$(find "$artifacts_dir" -maxdepth 1 -name 'purra-*.whl' -print -quit)"
"$python_bin" -m pip install \
  --disable-pip-version-check \
  --no-deps \
  --target "$python_site" \
  "$python_wheel"
PURRA_EXPECTED_SITE="$python_site" PYTHONPATH="$python_site" \
  "$python_bin" "$project_dir/python_smoke.py" > "$results_dir/python.json"
PURRA_EXPECTED_SITE="$python_site" PYTHONPATH="$python_site" \
  "$python_bin" "$project_dir/python_child_agent_smoke.py" \
  > "$results_dir/python-child-agent.json"

npm pack --loglevel=error --pack-destination "$artifacts_dir" "$source_dir/typescript"
typescript_tarball="$(find "$artifacts_dir" -maxdepth 1 -name 'purra-*.tgz' -print -quit)"
npm install --prefix "$project_dir" \
  --ignore-scripts \
  --no-audit \
  --no-fund \
  --no-package-lock \
  --no-save \
  --offline \
  "$typescript_tarball"
"$source_dir/typescript/node_modules/.bin/tsc" -p "$project_dir/tsconfig.json"
node "$project_dir/dist/typescript-smoke.js" > "$results_dir/typescript.json"
node "$project_dir/dist/typescript-child-agent-smoke.js" \
  > "$results_dir/typescript-child-agent.json"

node "$project_dir/compare-results.mjs" \
  "$results_dir/python.json" \
  "$results_dir/typescript.json"
node "$project_dir/compare-results.mjs" \
  "$results_dir/python-child-agent.json" \
  "$results_dir/typescript-child-agent.json"
