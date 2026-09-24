#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "26.9.4"
COORDS = [
    ("org.pipelineframework", "pipelineframework-runtime-parent", "pom"),
    ("org.pipelineframework", "pipelineframework", "jar"),
    ("org.pipelineframework", "pipelineframework-deployment", "jar"),
    ("org.pipelineframework", "pipelineframework-runtime-spring", "jar"),
    ("org.pipelineframework", "pipelineframework-release-maven-plugin", "jar"),
    ("org.pipelineframework", "cache-plugin", "jar"),
    ("org.pipelineframework", "persistence-plugin", "jar"),
    ("org.pipelineframework", "repository-plugin", "jar"),
]

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
        assert len(metadata["mavenArtifacts"]) == 8
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
