#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
hermes_home="${HERMES_HOME:-$HOME/.hermes}"
target="$hermes_home/plugins/sendblue"

mkdir -p "$(dirname "$target")"

if [[ -e "$target" && ! -L "$target" ]]; then
  echo "Refusing to replace existing non-symlink: $target" >&2
  echo "Move it aside, then rerun ./install.sh." >&2
  exit 1
fi

ln -sfn "$repo_dir" "$target"

echo "Installed Hermes Sendblue plugin at $target"
echo "Next:"
echo "  hermes plugins enable sendblue-platform"
echo "  hermes gateway setup"
echo "  hermes gateway restart"
