#!/usr/bin/env bash
set -euo pipefail

output_dir=${1:?usage: collect-candidate-files.sh OUTPUT_DIR [MAVEN_REPOSITORY]}
[[ ! -e "$output_dir" ]] || { echo "output directory already exists: $output_dir" >&2; exit 2; }
repo_root=$(cd "$(dirname "$0")/.." && pwd)
maven_repository=${2:-"$repo_root/.m2/repository"}
mkdir -p "$output_dir/repository"
version=$(python3 - "$repo_root/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET
print(ET.parse(sys.argv[1]).getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", ""))
PY
)
for artifact_id in pipelineframework-runtime-parent pipelineframework pipelineframework-deployment pipelineframework-runtime-spring pipelineframework-release-maven-plugin pipelineframework-release-producer pipelineframework-aws-durable-coordination-parent pipelineframework-aws-durable-coordination-model pipelineframework-aws-durable-coordination-host pipelineframework-aws-durable-coordination-deployment cache-plugin persistence-plugin repository-plugin; do
  group_path=org/pipelineframework
  artifact_dir="$maven_repository/$group_path/$artifact_id/$version"
  [[ -d "$artifact_dir" ]] || { echo "missing installed candidate coordinate: $artifact_id:$version" >&2; exit 1; }
  relative="$group_path/$artifact_id/$version"
  mkdir -p "$output_dir/repository/$(dirname "$relative")"
  destination="$output_dir/repository/$relative"
  mkdir -p "$destination"
  files=("$artifact_dir/$artifact_id-$version.pom")
  case "$artifact_id" in
    pipelineframework-runtime-parent|pipelineframework-aws-durable-coordination-parent) ;;
    *) files+=("$artifact_dir/$artifact_id-$version.jar") ;;
  esac
  for file in "${files[@]}"; do
    [[ -f "$file" ]] || { echo "missing candidate file: $file" >&2; exit 1; }
    cp "$file" "$destination/"
  done
done
