This producer integration fixture builds the existing Java21/Quarkus AWS Durable application, then packages the original host's raw coordinator/ingress group separately from its Quarkus action/work group. The driver ZIP contains dependency JARs and the exact compiler metadata, but not the application service classes or transformed action handlers at ZIP root. The action ZIP is the original Quarkus output. Shared dependency JARs are unchanged.

The original Release producer emits one schema2 Release containing both artifact digests and the existing ten entries. Only the action artifact owns the Echo semantic step. No second topology, application adapter or new runtime is introduced. The original host SAM still owns the two code groups.

Run from the runtime root with the current exact local dependency set:

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home ./mvnw -o -f release-maven-plugin/src/it/hosted-aws-durable-split/pom.xml clean verify -Dmaven.repo.local="$PWD/.m2/repository"
```

This proves compiler/package/producer compatibility. It does not qualify Cloud delivery, AWS execution, HTTP UI hosting, external connectors or live retained updates.
