import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.util.zip.ZipFile

def release = new JsonSlurper().parse(new File(basedir, 'app/target/pipeline-release.json'))
assert release.schemaVersion == 2
assert release.releaseVersion == 'rest-worker-command-release-1'
assert release.compiledTruthArtifactId == 'worker'
assert release.deployment.profile == 'aws-durable-java21-rest-worker-v1'
assert release.deployment.bindings.size() == 1
assert release.deployment.bindings[0].bindingId == 'command-effects'
assert release.deployment.bindings[0].capability == 'aws:dynamodb:command-effects'
assert release.deployment.entries.size() == 1
def entry = release.deployment.entries[0]
assert entry.entryId == 'worker'
assert entry.artifactId == 'worker'
assert entry.trigger == 'AWS_DURABLE_REST_WORKER'
assert entry.entryClass == 'io.quarkus.amazon.lambda.http.LambdaHttpHandler'
assert entry.bindingIds == ['command-effects']
assert release.artifacts.size() == 1
def artifact = release.artifacts[0]
assert artifact.artifactId == 'worker'
assert artifact.kind == 'lambda-zip'
assert artifact.stepIds == ['Echo']

def zipFile = new File(basedir, 'app/target/function.zip')
assert artifact.digest == 'sha256:' + MessageDigest.getInstance('SHA-256').digest(zipFile.bytes).encodeHex().toString()

new ZipFile(zipFile).withCloseable { zip ->
    def contractEntry = zip.getEntry('META-INF/pipeline/pipeline-contract.json')
    assert contractEntry != null
    def contract = new JsonSlurper().parse(zip.getInputStream(contractEntry))
    assert contract.platform == 'FUNCTION'
    assert contract.transport == 'REST'
    assert contract.steps.size() == 1
    assert contract.steps[0].authoredName == 'Echo'
    assert contract.steps[0].kind == 'command'

    def factsEntry = zip.getEntry('META-INF/tpf/runtime-capabilities.properties')
    assert factsEntry != null
    def facts = new Properties()
    facts.load(zip.getInputStream(factsEntry))
    assert facts.getProperty('commandEffectStore.provider') == 'dynamo'
    assert facts.getProperty('commandEffectStore.recoverySupported') == 'true'
    assert facts.getProperty('commandEffectStore.selectedBeans') == 'org.pipelineframework.command.DynamoCommandEffectStore'

    assert zip.getEntry('org/pipelineframework/pipeline/service/pipeline/ProcessEchoCommandClientStep.class') != null
    def httpJar = zip.getEntry(entry.entryPackagePath)
    assert httpJar != null : "Missing entry package ${entry.entryPackagePath}"
    def foundHandler = false
    new java.util.zip.ZipInputStream(zip.getInputStream(httpJar)).withCloseable { nested ->
        java.util.zip.ZipEntry member
        while ((member = nested.nextEntry) != null) {
            if (member.name == 'io/quarkus/amazon/lambda/http/LambdaHttpHandler.class') {
                foundHandler = true
                break
            }
        }
    }
    assert foundHandler : 'Missing LambdaHttpHandler.class inside declared entry package'
    assert zip.getEntry('org/pipelineframework/aws/durable/AwsDurableCoordinatorHandler.class') == null
}
return true
