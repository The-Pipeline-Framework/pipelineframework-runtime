This producer integration fixture builds the existing Java21/Quarkus AWS Durable application, then packages the original host's raw coordinator/ingress group separately from its Quarkus action/work group. The driver ZIP contains dependency JARs and the exact compiler metadata, but not the application service classes or transformed action handlers at ZIP root. The action ZIP is the original Quarkus output. Shared dependency JARs are unchanged.

The original Release producer emits one schema2 Release containing both artifact digests and the existing ten entries. Only the action artifact owns the Echo semantic step. No second topology, application adapter or new runtime is introduced. The original host SAM still owns the two code groups.

Run from the runtime root with the current exact local dependency set:

Use Java 21 or newer. Invoker filters the version placeholders in the source POM into `release-maven-plugin/target/it/` before building.

```sh
./mvnw -o -pl release-maven-plugin invoker:install invoker:run -Dinvoker.test=hosted-aws-durable-split -Dmaven.repo.local="$PWD/.m2/repository"
```

This proves compiler/package/producer compatibility. It does not qualify Cloud delivery, AWS execution, HTTP UI hosting, external connectors or live retained updates.

Filtered build outputs are under `release-maven-plugin/target/it/hosted-aws-durable-split/target/`: `pipeline-release.json`, `driver.zip` and `function.zip`.
