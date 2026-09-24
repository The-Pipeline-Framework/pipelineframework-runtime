import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.util.zip.ZipFile

def target = new File(basedir, 'target')
def jar = new File(target, 'release-descriptor-real-pipeline-it-1.0.0.jar')
def releaseFile = new File(target, 'pipeline-release.json')
assert jar.isFile()
assert releaseFile.isFile()

def release = new JsonSlurper().parse(releaseFile)
assert release.schemaVersion == 1
assert release.releaseVersion == 'it-release-1'
assert release.artifacts*.artifactId == ['release-descriptor-real-pipeline-it']
assert release.artifacts*.kind == ['jar']
assert release.artifacts[0].uri == 'maven://org.pipelineframework.it:release-descriptor-real-pipeline-it:1.0.0'
assert release.artifacts[0].stepIds == ['Echo']

def sha256 = MessageDigest.getInstance('SHA-256').digest(jar.bytes).encodeHex().toString()
assert release.artifacts[0].digest == "sha256:${sha256}"

new ZipFile(jar).withCloseable { zip ->
    def entry = zip.getEntry('META-INF/pipeline/pipeline-contract.json')
    assert entry != null
    def contract = new JsonSlurper().parse(zip.getInputStream(entry))
    assert release.pipelineId == contract.pipelineId
    assert release.contractVersion == contract.contractVersion
}

return true
