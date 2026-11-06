import groovy.json.JsonSlurper
import java.util.zip.ZipFile
assert new File(basedir, 'target/function.zip').isFile()
def release = new JsonSlurper().parse(new File(basedir, 'target/pipeline-release.json'))
assert release.schemaVersion == 2
assert release.deployment.profile == 'aws-durable-java21-v1'
assert release.deployment.entries.size() == 10
assert release.deployment.entries*.trigger.toSet().size() == 10
assert release.deployment.entries.every { it.packagePath == '.' }
assert release.deployment.entries.count { it.entryPackagePath == '.' } == 8
assert release.deployment.bindings.empty
assert release.deployment.check.input.payloadEncoding == 'application/tpf-transition+json'
new ZipFile(new File(basedir, 'target/function.zip')).withCloseable { zip ->
  assert zip.getEntry('META-INF/pipeline/pipeline-contract.json')
  def lambdaRuntime = zip.getEntry('lib/io.quarkus.quarkus-amazon-lambda-3.39.2.jar')
  assert lambdaRuntime
  def handlerPresent = false
  new java.util.zip.ZipInputStream(zip.getInputStream(lambdaRuntime)).withCloseable { nested ->
    java.util.zip.ZipEntry member
    while ((member = nested.nextEntry) != null) {
      if (member.name == 'io/quarkus/amazon/lambda/runtime/QuarkusStreamHandler.class') handlerPresent = true
    }
  }
  assert handlerPresent
  assert release.deployment.entries.every { it.entryPackagePath == '.' ? zip.getEntry(it.entryClass.replace('.', '/') + '.class') : zip.getEntry(it.entryPackagePath) }
}
return true
