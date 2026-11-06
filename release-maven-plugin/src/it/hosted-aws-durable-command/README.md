# Generated Command package qualification

This runtime-owned integration fixture builds an ordinary provider artifact and a Java Lambda application through the existing compiler, Quarkus augmentation and Release producer. It uses the original two AWS Durable code groups. It has no external connection or credentials: the fixture operation returns a typed value and counts dispatches.

The invoker runs `clean install -Dpipeline.command.effect-store.provider=dynamo`. Its verification checks exact ZIP hashes, generated application code and the actual selected retained CDI store fact. The default-memory negative is produced by running the app with `-Dpipeline.command.effect-store.provider=memory`; declaring the durable effects binding must make Release production fail.

The separate `app/proof/run.py http://127.0.0.1:<port>` harness requires a local DynamoDB endpoint. It loads the actual generated class from function.zip, sends signed messages through the existing AWS transition handler, executes the real portable pipeline runner and Dynamo effect store, then recreates the store/worker and proves recorded output with one provider dispatch. Only SQS response transport and runtime wiring are fixture boundaries. This is not AWS deployment, complete coordinator execution, or external connector credential qualification.

Hosted admission remains restricted until the compiler exports explicit external-configuration requirements and the owner profile validates them. Product Set, Query, Await and ConnectionRef support are not established by this fixture.
