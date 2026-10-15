#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = ET.parse(ROOT / "pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version").removesuffix("-SNAPSHOT")
COORDS = [
    ("org.pipelineframework", "pipelineframework-runtime-parent", "pom"),
    ("org.pipelineframework", "pipelineframework", "jar"),
    ("org.pipelineframework", "pipelineframework-deployment", "jar"),
    ("org.pipelineframework", "pipelineframework-runtime-spring", "jar"),
    ("org.pipelineframework", "pipelineframework-release-maven-plugin", "jar"),
    ("org.pipelineframework", "pipelineframework-release-producer", "jar"),
    ("org.pipelineframework", "pipelineframework-aws-durable-coordination-parent", "pom"),
    ("org.pipelineframework", "pipelineframework-aws-durable-coordination-model", "jar"),
    ("org.pipelineframework", "pipelineframework-aws-durable-coordination-host", "jar"),
    ("org.pipelineframework", "pipelineframework-aws-durable-coordination-deployment", "jar"),
    ("org.pipelineframework", "cache-plugin", "jar"),
    ("org.pipelineframework", "persistence-plugin", "jar"),
    ("org.pipelineframework", "repository-plugin", "jar"),
]

# Exercise the real collection boundary, including its POM-only AWS parent.
snapshot = VERSION + "-SNAPSHOT"
with tempfile.TemporaryDirectory(prefix="tpf-candidate-collection-") as temp:
    installed = pathlib.Path(temp) / "installed"
    expected = set()
    for group, artifact, packaging in COORDS:
        directory = installed / pathlib.Path(*group.split(".")) / artifact / snapshot
        directory.mkdir(parents=True)
        extensions = ("pom", "jar") if packaging == "jar" else ("pom",)
        for extension in extensions:
            file = directory / f"{artifact}-{snapshot}.{extension}"
            file.write_bytes(b"installed candidate fixture")
            expected.add(file.relative_to(installed))
    output = pathlib.Path(temp) / "collected"
    subprocess.run(["bash", str(ROOT / "scripts/collect-candidate-files.sh"), str(output), str(installed)],
                   check=True, cwd=ROOT)
    assert {p.relative_to(output / "repository") for p in (output / "repository").rglob("*") if p.is_file()} == expected
    for _, artifact, packaging in [c for c in COORDS if "aws-durable" in c[1]]:
        extension = "jar" if packaging == "jar" else "pom"
        file = installed / "org/pipelineframework" / artifact / snapshot / f"{artifact}-{snapshot}.{extension}"
        content = file.read_bytes()
        file.unlink()
        rejected = subprocess.run(
            ["bash", str(ROOT / "scripts/collect-candidate-files.sh"), str(pathlib.Path(temp) / artifact), str(installed)],
            cwd=ROOT, capture_output=True)
        assert rejected.returncode != 0, f"missing AWS candidate file must fail: {artifact}"
        file.write_bytes(content)

for event, candidate_type, number in [("pull_request", "pr", "42"), ("push", "main", "")]:
    sha = "0123456789abcdef0123456789abcdef01234567"
    candidate = f"{VERSION}-pr.{number}.{sha[:12]}" if number else f"{VERSION}-main.{sha[:12]}"
    with tempfile.TemporaryDirectory(prefix="tpf-candidate-metadata-") as temp:
        candidate_root = pathlib.Path(temp)
        repository = candidate_root / "repository"
        for group, artifact, packaging in COORDS:
            directory = repository / pathlib.Path(*group.split(".")) / artifact / candidate
            directory.mkdir(parents=True)
            ns = "http://maven.apache.org/POM/4.0.0"
            if artifact == "pipelineframework":
                pom = (f'<project xmlns="{ns}"><modelVersion>4.0.0</modelVersion><parent><groupId>{group}</groupId>'
                       f'<artifactId>pipelineframework-runtime-parent</artifactId><version>{candidate}</version>'
                       f'</parent><artifactId>{artifact}</artifactId><packaging>{packaging}</packaging></project>\n')
            else:
                pom = f'<project xmlns="{ns}"><modelVersion>4.0.0</modelVersion><groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{candidate}</version><packaging>{packaging}</packaging></project>\n'
            (directory / f"{artifact}-{candidate}.pom").write_text(pom)
            if packaging == "jar":
                (directory / f"{artifact}-{candidate}.jar").write_bytes(b"deterministic candidate jar fixture")
        env = os.environ | {
            "GITHUB_REPOSITORY": "The-Pipeline-Framework/pipelineframework-runtime",
            "SOURCE_REPOSITORY": "Contributor/pipelineframework-runtime" if number else "The-Pipeline-Framework/pipelineframework-runtime",
            "SOURCE_SHA": sha,
            "GITHUB_EVENT_NAME": event,
            "PULL_REQUEST_NUMBER": number,
            "GITHUB_RUN_ID": "1234",
            "GITHUB_RUN_ATTEMPT": "2",
        }
        (candidate_root / "candidate-manifest.json").write_text(json.dumps({"candidateVersion": candidate}))
        subprocess.run(["python3", str(ROOT / "scripts/create-build-metadata.py"), str(candidate_root)],
                       check=True, cwd=ROOT, env=env)
        metadata = json.loads((candidate_root / "build-metadata.json").read_text())
        assert metadata["candidateVersion"] == candidate
        assert metadata["pullRequestNumber"] == (42 if number else None)
        assert len(metadata["mavenArtifacts"]) == len(COORDS)
        if number:
            # Only the external Maven CLI is substituted: execute the real publisher
            # and require every owned coordinate, including the POM-only AWS parent.
            publisher = candidate_root / "publisher"
            (publisher / "scripts").mkdir(parents=True)
            (publisher / "scripts/publish-candidate-files.sh").write_bytes(
                (ROOT / "scripts/publish-candidate-files.sh").read_bytes())
            wrapper = publisher / "mvnw"
            wrapper.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
if "-Dtransitive=false" in args:
    coordinate = next(a.removeprefix("-Dartifact=") for a in args if a.startswith("-Dartifact="))
    if os.environ.get("FIXTURE_DENIED") == "true":
        print("Authentication denied")
    else:
        print("Could not find artifact " + coordinate + " in github-candidates")
    sys.exit(1)
assert "org.apache.maven.plugins:maven-deploy-plugin:3.1.4:deploy-file" in args
properties = dict(a[2:].split("=", 1) for a in args if a.startswith("-D") and "=" in a)
assert pathlib.Path(properties["file"]).is_file(), properties
assert pathlib.Path(properties["pomFile"]).is_file()
with open(os.environ["FIXTURE_PUBLISH_CALLS"], "a") as f:
    f.write(json.dumps(properties) + "\\n")
''')
            wrapper.chmod(0o755)
            calls = publisher / "calls.jsonl"
            publisher_env = env | {"FIXTURE_PUBLISH_CALLS": str(calls)}
            publication = subprocess.run(["bash", str(publisher / "scripts/publish-candidate-files.sh"), str(candidate_root)],
                                         cwd=publisher, env=publisher_env, capture_output=True)
            assert publication.returncode == 0, publication.stderr.decode()
            published = [json.loads(line) for line in calls.read_text().splitlines()]
            assert [(c["groupId"], c["artifactId"], c["packaging"]) for c in published] == COORDS
            assert all(c["version"] == candidate for c in published)
            calls.unlink()
            denied = subprocess.run(["bash", str(publisher / "scripts/publish-candidate-files.sh"), str(candidate_root)],
                                    cwd=publisher, env=publisher_env | {"FIXTURE_DENIED": "true"}, capture_output=True)
            assert denied.returncode != 0 and not calls.exists(), "registry denial must not become permission to publish"
        assert any(item["artifactId"] == "pipelineframework-release-producer" for item in metadata["mavenArtifacts"])
        producer_pom = repository / "org/pipelineframework/pipelineframework-release-producer" / candidate / f"pipelineframework-release-producer-{candidate}.pom"
        producer_bytes = producer_pom.read_bytes()
        producer_pom.unlink()
        rejected = subprocess.run(["python3", str(ROOT / "scripts/create-build-metadata.py"), str(candidate_root)],
                                  cwd=ROOT, env=env, capture_output=True)
        assert rejected.returncode != 0, "missing release-producer POM must fail before publication"
        producer_pom.write_bytes(producer_bytes)
        for artifact in metadata["mavenArtifacts"]:
            for item in artifact["files"]:
                path = repository / pathlib.Path(*artifact["groupId"].split(".")) / artifact["artifactId"] / candidate / item["name"]
                assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
        final_env = env | {"GITHUB_RUN_ID": "5678", "GITHUB_RUN_ATTEMPT": "1"}
        subprocess.run(["python3", str(ROOT / "scripts/finalize-candidate-manifest.py"), str(candidate_root)],
                       check=True, cwd=candidate_root, env=final_env)
        manifest_bytes = (candidate_root / "candidate-manifest/candidate-manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        assert set(manifest) == {"schemaVersion", "repository", "component", "sourceSha", "pullRequestNumber", "candidateVersion", "provenance", "mavenArtifacts", "images"}
        assert manifest["provenance"]["publication"]["event"] == "workflow_run"
        assert manifest["provenance"]["build"]["event"] == event
        assert manifest["images"] == []
        event_doc = json.loads((candidate_root / "candidate-event/event.json").read_text())
        assert set(event_doc) == {"schema_version", "source_repository", "source_sha", "pull_request_number", "component", "candidate_version", "publication_run_id", "manifest_sha256", "compatibility_set_id"}
        assert event_doc["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
        assert event_doc["publication_run_id"] == 5678
        assert event_doc["source_repository"] == "The-Pipeline-Framework/pipelineframework-runtime"
        if number:
            assert env["SOURCE_REPOSITORY"] != event_doc["source_repository"]
        pr_json = candidate_root / "current-pr.json"
        pr_json.write_text(json.dumps({
            "state": "open", "number": 42,
            "head": {"sha": sha, "ref": "candidate-branch", "repo": {"full_name": env["SOURCE_REPOSITORY"]}},
            "labels": [{"name": "safe-to-system-test"}],
        }))
        validate_env = env | {
            "BUILD_RUN_ID": "1234", "BUILD_RUN_ATTEMPT": "2", "BUILD_RUN_EVENT": event,
            "BUILD_RUN_HEAD_SHA": sha, "BUILD_RUN_HEAD_BRANCH": "candidate-branch" if number else "main",
            "BUILD_RUN_PATH": ".github/workflows/tpf-candidate-build.yml",
            "BUILD_RUN_REPOSITORY": "The-Pipeline-Framework/pipelineframework-runtime",
            "BUILD_PR_NUMBER": number, "BUILD_ASSOCIATED_PR_NUMBERS": "[42]" if number else "[]",
        }
        if number:
            validate_env["BUILD_RUN_HEAD_SHA"] = "fedcba9876543210fedcba9876543210fedcba98"
        subprocess.run(["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(candidate_root), str(pr_json)],
                       check=True, cwd=ROOT, env=validate_env)
        if number:
            missing_association_env = validate_env | {"BUILD_ASSOCIATED_PR_NUMBERS": "[]"}
            rejected = subprocess.run(
                ["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(candidate_root), str(pr_json)],
                cwd=ROOT, env=missing_association_env, capture_output=True,
            )
            assert rejected.returncode != 0, "empty workflow_run PR association must be rejected"
            fork_without_label = json.loads(pr_json.read_text())
            fork_without_label["labels"] = []
            pr_json.write_text(json.dumps(fork_without_label))
            rejected = subprocess.run(
                ["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(candidate_root), str(pr_json)],
                cwd=ROOT, env=validate_env, capture_output=True,
            )
            assert rejected.returncode != 0, "fork PR without safe-to-system-test must be rejected"
            (repository / "unexpected.txt").write_text("not allowlisted")
            rejected = subprocess.run(
                ["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(candidate_root), str(pr_json)],
                cwd=ROOT, env=validate_env, capture_output=True,
            )
            assert rejected.returncode != 0, "unexpected candidate files must be rejected"
        (candidate_root / "repository" / "org" / "pipelineframework" / COORDS[1][1] / candidate / f"{COORDS[1][1]}-{candidate}.jar").write_bytes(b"changed")
        rejected = subprocess.run(
            ["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(candidate_root), str(pr_json)],
            cwd=ROOT, env=validate_env, capture_output=True,
        )
        assert rejected.returncode != 0, "checksum mismatch must be rejected"
print("synthetic PR and main candidate metadata passed")
