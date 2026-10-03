assert !new File(basedir, "target/pipeline-release.json").exists()
assert new File(basedir, "build.log").text.contains("Skipping Pipeline Release Descriptor production")
