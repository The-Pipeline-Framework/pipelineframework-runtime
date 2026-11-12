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
  release.deployment.entries.each { entry ->
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
      assert found : "Missing ${classPath} for entry ${entry.entryId}" }
}
// Start the actual Lambda package with only the transition worker configuration.
def unpacked = new File(basedir, 'target/transition-worker-smoke')
unpacked.mkdirs()
new ZipFile(new File(basedir, 'target/function.zip')).withCloseable { zip ->
    zip.entries().each { entry ->
        def destination = unpacked.toPath().resolve(entry.name).normalize()
        assert destination.startsWith(unpacked.toPath())
        if (entry.directory) {
            java.nio.file.Files.createDirectories(destination)
        } else {
            java.nio.file.Files.createDirectories(destination.parent)
            zip.getInputStream(entry).withCloseable { source ->
                java.nio.file.Files.copy(source, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }
}
def javaExecutable = new File(System.getProperty('java.home'), 'bin/java').path
def classpath = [unpacked.path, new File(unpacked, 'lib/*').path].join(File.pathSeparator)
def builder = new ProcessBuilder(javaExecutable, '--enable-preview', '--source', '21',
    '-Djava.util.logging.manager=org.jboss.logmanager.LogManager', '-cp', classpath,
    new File(basedir, 'TransitionWorkerSmoke.java').path, release.pipelineId, release.contractVersion, release.releaseVersion)
def environment = builder.environment()
environment.keySet().removeIf { key -> key.startsWith('AWS_') || key.startsWith('TPF_') || key.startsWith('PIPELINE_') || key.startsWith('QUARKUS_') }
environment.putAll([
    AWS_ACCESS_KEY_ID: 'test-access-key',
    AWS_SECRET_ACCESS_KEY: 'test-secret-key',
    AWS_REGION: 'us-east-2',
    AWS_EC2_METADATA_DISABLED: 'true',
    QUARKUS_LAMBDA_HANDLER: 'aws-durable-transition-worker',
    QUARKUS_HTTP_HOST_ENABLED: 'false',
    QUARKUS_GRPC_SERVER_USE_SEPARATE_SERVER: 'false',
    TPF_AWS_DURABLE_TRANSITION_WORKER_ONLY: 'true',
    PIPELINE_ORCHESTRATOR_MODE: 'SYNC',
    PIPELINE_ORCHESTRATOR_PROCESS_LOOPS_DISABLED: 'true',
    PIPELINE_ORCHESTRATOR_PIPELINE_ID: release.pipelineId,
    PIPELINE_ORCHESTRATOR_RELEASE_VERSION: release.releaseVersion,
    PIPELINE_ORCHESTRATOR_SQS_REGION: 'us-east-2',
    PIPELINE_ORCHESTRATOR_WORKER_ALLOWED_PAYLOAD_PREFIXES: 'com.example.echo.',
    PIPELINE_ORCHESTRATOR_WORKER_SQS_SHARED_SECRET_REF: 'env:TRANSITION_SIGNING_KEY',
    TRANSITION_SIGNING_KEY: 'local-transition-worker-test-key'
])
def workerLog = new File(basedir, 'target/transition-worker-smoke.log')
def worker = builder.redirectErrorStream(true).redirectOutput(workerLog).start()
try {
    assert worker.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) : "Packaged transition worker timed out: ${workerLog.text}"
    assert worker.exitValue() == 0 : "Packaged transition worker failed: ${workerLog.text}"
} finally {
    if (worker.isAlive()) {
        worker.destroyForcibly()
    }
}
return true
