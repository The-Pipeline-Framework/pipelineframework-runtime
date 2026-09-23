#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: prepare-candidate.sh pull_request|push PR_NUMBER SHA}
pr_number=${2:--}
sha=${3:?}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
base_version=$(python3 - "$repo_root/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
version = root.findtext("{http://maven.apache.org/POM/4.0.0}version", "")
if not version.endswith("-SNAPSHOT"):
    raise SystemExit("root project version must end in -SNAPSHOT")
print(version.removesuffix("-SNAPSHOT"))
PY
)
case "$mode" in
  pull_request) version_mode=pr ;;
  push) version_mode=main ;;
  *) echo "mode must be pull_request or push" >&2; exit 2 ;;
esac
candidate=$(bash "$repo_root/scripts/candidate-version.sh" "$version_mode" "$base_version" "$pr_number" "$sha")

cd "$repo_root"

# Maven's versions:set changes the aggregator and child parent declarations.
"$repo_root/mvnw" -B -N -Dmaven.repo.local="$repo_root/.m2/repository" \
  -Dmaven.deploy.skip=true -Dgpg.skip=true -Dtpf.flatten.skip=true \
  org.codehaus.mojo:versions-maven-plugin:2.22.0:set \
  -DnewVersion="$candidate" -DprocessAllModules=true -DgenerateBackupPoms=false
python3 - "$repo_root/pom.xml" "$candidate" <<'PY'
import pathlib, re, sys
pom = pathlib.Path(sys.argv[1])
candidate = sys.argv[2]
source = pom.read_text()
names = "pipelineframework|pipelineframework-deployment|pipelineframework-runtime-spring|cache-plugin|persistence-plugin|repository-plugin"
pattern = re.compile(r"(<dependency\b[^>]*>.*?<groupId>\s*org\.pipelineframework\s*</groupId>.*?<artifactId>\s*(?:" + names + r")\s*</artifactId>.*?<version>\s*)[^<]+(\s*</version>.*?</dependency>)", re.DOTALL)
updated = pattern.sub(lambda match: match.group(1) + candidate + match.group(2), source)
pom.write_text(updated)
PY
echo "candidate=$candidate" >> "${GITHUB_OUTPUT:-/dev/null}"
python3 - "$candidate" "$repo_root" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[2]) / ".candidate-version.json"
path.write_text(json.dumps({"candidateVersion": sys.argv[1]}, sort_keys=True) + "\n")
PY
if [[ -n "${RUNNER_TEMP:-}" ]]; then
  cp "$repo_root/.candidate-version.json" "$RUNNER_TEMP/tpf-candidate-version.json"
else
  cp "$repo_root/.candidate-version.json" "$repo_root/candidate-manifest.json"
fi
