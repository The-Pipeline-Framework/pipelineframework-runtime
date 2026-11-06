import groovy.json.JsonSlurper
import java.util.zip.ZipFile
import java.security.MessageDigest

def release = new JsonSlurper().parse(new File(basedir, 'app/target/pipeline-release.json'))
assert release.schemaVersion == 2
assert release.releaseVersion == 'native-command-release-5'
assert release.compiledTruthArtifactId == 'actions'
assert release.deployment.entries.size() == 10
assert release.artifacts*.artifactId.toSet() == ['driver', 'actions'].toSet()
assert release.artifacts.find { it.artifactId == 'driver' }.stepIds.empty
assert release.artifacts.find { it.artifactId == 'actions' }.stepIds == ['Echo']
assert release.deployment.entries.findAll { it.trigger in ['AWS_DURABLE_COORDINATOR','AWS_DURABLE_INGRESS'] }.every { it.artifactId == 'driver' }
assert release.deployment.entries.findAll { it.artifactId == 'driver' }*.trigger.toSet().containsAll(['AWS_DURABLE_COORDINATOR', 'AWS_DURABLE_INGRESS'])
assert release.deployment.entries.findAll { !(it.trigger in ['AWS_DURABLE_COORDINATOR','AWS_DURABLE_INGRESS']) }.every { it.artifactId == 'actions' }
assert release.artifacts*.digest.toSet().size() == 2
for (artifact in release.artifacts) {
  def file = new File(basedir, artifact.artifactId == 'driver' ? 'app/target/driver.zip' : 'app/target/function.zip')
  assert artifact.digest == 'sha256:' + MessageDigest.getInstance('SHA-256').digest(file.bytes).encodeHex().toString()
  new ZipFile(file).withCloseable { zip ->
    assert zip.getEntry('META-INF/pipeline/pipeline-contract.json')
    release.deployment.entries.findAll { it.artifactId == artifact.artifactId }.each { entry ->
      def classPath = entry.entryClass.replace('.', '/') + '.class'
      if (entry.entryPackagePath == '.') {
        assert zip.getEntry(classPath) != null : "Missing ${classPath} for entry ${entry.entryId}"
        return
      }
      def jar = zip.getEntry(entry.entryPackagePath)
      assert jar : "Missing entry package ${entry.entryPackagePath}"
      def found = false
      new java.util.zip.ZipInputStream(zip.getInputStream(jar)).withCloseable { nested ->
        java.util.zip.ZipEntry member
        while ((member = nested.nextEntry) != null) {
          if (member.name == classPath) { found = true; break }
        }
      }
      assert found : "Missing ${classPath} for entry ${entry.entryId}"
    }
    if (artifact.artifactId == 'driver') {
      assert !zip.getEntry('com/example/echo/EchoService.class')
      assert !zip.getEntry('org/pipelineframework/aws/durable/AwsDurableActionGatewayHandler.class')
    } else {
      assert zip.getEntry('org/pipelineframework/pipeline/service/pipeline/ProcessEchoCommandClientStep.class')
      def bindings = new JsonSlurper().parse(zip.getInputStream(zip.getEntry('META-INF/pipeline/connector-bindings.json')))
      assert bindings.bindings.every { it.externalConfigurationReferenceFields == [] && it.operations.every { op -> op.externalConfigurationReferenceFields == [] } }
      def facts = new Properties()
      facts.load(zip.getInputStream(zip.getEntry('META-INF/tpf/runtime-capabilities.properties')))
      assert facts.getProperty('commandEffectStore.provider') == 'dynamo'
      assert facts.getProperty('commandEffectStore.recoverySupported') == 'true'
      assert facts.getProperty('commandEffectStore.selectedBeans') == 'org.pipelineframework.command.DynamoCommandEffectStore'
      assert zip.getEntry('org/pipelineframework/aws/durable/AwsDurableActionGatewayHandler.class')
    }
  }
}
return true
