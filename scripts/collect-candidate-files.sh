#!/usr/bin/env bash
set -euo pipefail

output_dir=${1:?usage: collect-candidate-files.sh OUTPUT_DIR}
[[ ! -e "$output_dir" ]] || { echo "output directory already exists: $output_dir" >&2; exit 2; }
repo_root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$output_dir/repository"
version=$(python3 - "$repo_root/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET
print(ET.parse(sys.argv[1]).getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", ""))
PY
)
for artifact_id in pipelineframework-runtime-parent pipelineframework pipelineframework-deployment pipelineframework-runtime-spring cache-plugin persistence-plugin repository-plugin; do
  group_path=org/pipelineframework
  artifact_dir="$repo_root/.m2/repository/$group_path/$artifact_id/$version"
  [[ -d "$artifact_dir" ]] || { echo "missing installed candidate coordinate: $artifact_id:$version" >&2; exit 1; }
  relative="$group_path/$artifact_id/$version"
  mkdir -p "$output_dir/repository/$(dirname "$relative")"
  destination="$output_dir/repository/$relative"
  mkdir -p "$destination"
  artifact_id=$(basename "$(dirname "$artifact_dir")")
  for file in "$artifact_dir/$artifact_id-$version.pom" "$artifact_dir/$artifact_id-$version.jar"; do
    [[ -f "$file" ]] || { [[ "$artifact_id" == pipelineframework-runtime-parent && "$file" == *.jar ]] && continue; echo "missing candidate file: $file" >&2; exit 1; }
    cp "$file" "$destination/"
  done
  if [[ "$artifact_id" != pipelineframework-runtime-parent && ( ! -f "$destination/$artifact_id-$version.jar" || ! -f "$destination/$artifact_id-$version.pom" ) ]]; then
    echo "missing required published candidate files for $artifact_id" >&2
    exit 1
  fi
done
