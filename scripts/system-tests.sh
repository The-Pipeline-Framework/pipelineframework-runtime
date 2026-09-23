#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  candidate-version)
    mode=${2:?usage: system-tests.sh candidate-version pull_request|push PR_NUMBER SHA}
    pr_number=${3:--}
    sha=${4:?}
    case "$mode" in
      pull_request) mode=pr ;;
      push) mode=main ;;
      *) echo "event must be pull_request or push" >&2; exit 2 ;;
    esac
    current_version=$(python3 - <<'PY'
import xml.etree.ElementTree as ET
root = ET.parse("pom.xml").getroot()
print(root.findtext("{http://maven.apache.org/POM/4.0.0}version", ""))
PY
)
    base_version=${current_version%-SNAPSHOT}
    base_version=${base_version%%-pr.*}
    base_version=${base_version%%-main.*}
    actual=$(bash scripts/candidate-version.sh "$mode" "$base_version" "$pr_number" "$sha")
    if [[ "$mode" == pr ]]; then
      expected="${base_version}-pr.${pr_number}.${sha:0:12}"
    elif [[ "$mode" == main ]]; then
      expected="${base_version}-main.${sha:0:12}"
    else
      exit 2
    fi
    [[ "$actual" == "$expected" ]] || exit 1
    ;;
  reactor-coordinates)
    python3 - <<'PY'
import pathlib
import xml.etree.ElementTree as ET

ns = {"m": "http://maven.apache.org/POM/4.0.0"}
root_pom = pathlib.Path("pom.xml")
expected = ET.parse(root_pom).getroot().findtext("m:version", namespaces=ns)
assert expected and (expected.endswith("-SNAPSHOT") or "-pr." in expected or "-main." in expected)
for pom in [root_pom, *sorted(pathlib.Path(".").glob("*/pom.xml")), *sorted(pathlib.Path("plugins").glob("**/pom.xml"))]:
    root = ET.parse(pom).getroot()
    parent = root.find("m:parent", ns)
    if parent is not None:
        assert parent.findtext("m:version", namespaces=ns) == expected, pom
    if root.findtext("m:artifactId", namespaces=ns) in {
        "pipelineframework-runtime-parent", "pipelineframework", "pipelineframework-deployment",
        "pipelineframework-runtime-spring", "cache-plugin", "persistence-plugin", "repository-plugin",
    }:
        version = root.findtext("m:version", namespaces=ns)
        if version is None:
            version = root.findtext("m:parent/m:version", namespaces=ns)
        assert version == expected, pom
PY
    ;;
  reactor-dependencies)
    python3 - <<'PY'
import pathlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import re

candidate = "3.2.1-pr.42.0123456789ab"
with tempfile.TemporaryDirectory(prefix="tpf-reactor-dependency-test-") as temporary:
    root = pathlib.Path(temporary)
    (root / "pom.xml").write_text("""<project><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-parent</artifactId><version>3.2.1-SNAPSHOT</version></project>""")
    (root / "consumer").mkdir()
    (root / "runtime").mkdir()
    (root / "deployment").mkdir()
    (root / "consumer/pom.xml").write_text("""<project><groupId>org.pipelineframework</groupId><artifactId>consumer</artifactId><parent><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-parent</artifactId><version>3.2.1-SNAPSHOT</version></parent><dependencies><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework</artifactId><version>3.2.1-SNAPSHOT</version></dependency><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-deployment</artifactId><version>3.2.1-SNAPSHOT</version></dependency><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-compiler</artifactId><version>26.9.4-SNAPSHOT</version></dependency><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-spi</artifactId><version>26.9.4-SNAPSHOT</version></dependency></dependencies></project>""")
    (root / "deployment/pom.xml").write_text("""<project><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-deployment</artifactId><parent><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-parent</artifactId><version>3.2.1-SNAPSHOT</version></parent></project>""")
    (root / "runtime/pom.xml").write_text("""<project><groupId>org.pipelineframework</groupId><artifactId>pipelineframework</artifactId><parent><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-parent</artifactId><version>3.2.1-SNAPSHOT</version></parent></project>""")
    subprocess.run(["python3", str(pathlib.Path("scripts/rewrite-reactor-dependencies.py").resolve()), str(root), candidate], check=True)
    pom = ET.parse(root / "consumer/pom.xml").getroot()
    dependencies = pom.find("dependencies")
    assert dependencies[0].findtext("version") == candidate, ET.tostring(dependencies[0], encoding="unicode")
    assert dependencies[1].findtext("version") == candidate, ET.tostring(dependencies[1], encoding="unicode")
    assert dependencies[2].findtext("version") == "26.9.4-SNAPSHOT", ET.tostring(dependencies[2], encoding="unicode")
    assert dependencies[3].findtext("version") == "26.9.4-SNAPSHOT", ET.tostring(dependencies[3], encoding="unicode")
PY
    ;;
  *) echo "usage: system-tests.sh candidate-version pr|main PR_NUMBER SHA | reactor-coordinates | reactor-dependencies" >&2; exit 2 ;;
esac
