This is a real Java 21 application compiled by the TPF compiler and augmented by Quarkus's existing Amazon Lambda packaging. It uses the existing AWS Durable host and the host-owned `aws-durable-java21-v1` profile. This unrelated application normalizes text and then counts words in two compiler-generated steps.

From the runtime root, with exact current runtime/contracts/compiler/plugin artifacts installed in the repository cache:

Use Java 21 or newer. Invoker filters the version placeholders in the source POM into `release-maven-plugin/target/it/` before building.

```sh
./mvnw -o -pl release-maven-plugin invoker:install invoker:run -Dinvoker.test=hosted-aws-durable-word-count -Dmaven.repo.local="$PWD/.m2/repository"
```

The producer writes `release-maven-plugin/target/it/hosted-aws-durable-word-count/target/pipeline-release.json` from the actual `function.zip` in that same output directory, with ten unique existing host triggers and a typed JSON canary. Quarkus places application Compiled Truth and transformed handlers at ZIP root; coordinator and ingress stay in its modified host dependency. Entry paths record those observed package facts. No external app resource bindings are required by this pure transformation.

Building validates package/Contract/entry selection; it does not prove AWS execution or live readiness. Cloud supplies environment-owned tables, pinned signing keys, management-writer/workload-readonly permissions, retained version grants, and deployment observations before registration/activation.

For local Cloud artifact resolution without publication, install the produced ZIP at its original descriptor Maven coordinate:

```sh
./mvnw -o -N org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file -Dfile=release-maven-plugin/target/it/hosted-aws-durable-word-count/target/function.zip -DgroupId=org.pipelineframework.it -DartifactId=hosted-aws-durable-word-count-it -Dversion=1.0.0 -Dpackaging=zip -DgeneratePom=true -Dmaven.repo.local="$PWD/.m2/repository"
```

The original producer URI is `maven:org.pipelineframework.it:hosted-aws-durable-word-count-it:zip:1.0.0`. Local installation is not registry publication.
