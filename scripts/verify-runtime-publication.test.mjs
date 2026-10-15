import assert from 'node:assert/strict';
import test from 'node:test';
import { assertEffectivePublication, expectedDeployability } from './verify-runtime-publication.mjs';

const coordination = ['parent', 'model', 'host', 'deployment']
  .map((suffix) => `pipelineframework-aws-durable-coordination-${suffix}`);
const consumerProofs = ['pipelineframework-spring-smoke-tests', 'pipelineframework-spring-blocking-smoke-tests',
  'pipelineframework-aws-durable-proof-parent', 'pipelineframework-aws-durable-proof-driver',
  'pipelineframework-aws-durable-proof-action-host', 'pipelineframework-aws-durable-proof-fault-tests'];

function effectivePom(overrides = new Map()) {
  return `<projects>${[...expectedDeployability].map(([artifactId, expected]) => {
    const deployable = overrides.get(artifactId) ?? expected;
    return `<project><groupId>org.pipelineframework</groupId><artifactId>${artifactId}</artifactId>
      <properties><maven.deploy.skip>${!deployable}</maven.deploy.skip></properties>
      <build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId>
      <configuration><skipPublishing>${!deployable}</skipPublishing></configuration>
      </plugin></plugins></build></project>`;
  }).join('')}</projects>`;
}

test('publishes the complete AWS Durable coordination dependency closure', () => {
  for (const artifactId of coordination) assert.equal(expectedDeployability.get(artifactId), true, artifactId);
  assert.doesNotThrow(() => assertEffectivePublication(effectivePom()));
});

test('excludes relocated application proofs from the runtime reactor', () => {
  for (const artifactId of consumerProofs) assert.equal(expectedDeployability.has(artifactId), false, artifactId);
});

for (const artifactId of coordination) {
  test(`rejects publication eligibility drift for ${artifactId}`, () => {
    assert.throws(() => assertEffectivePublication(effectivePom(
      new Map([[artifactId, !expectedDeployability.get(artifactId)]])
    )), /effective deployability drift/);
  });
}

test('still rejects undeclared reactor projects', () => {
  const xml = effectivePom().replace('</projects>', `<project>
    <groupId>org.pipelineframework</groupId><artifactId>undeclared-module</artifactId>
    <properties><maven.deploy.skip>false</maven.deploy.skip></properties></project></projects>`);
  assert.throws(() => assertEffectivePublication(xml), /unexpected org.pipelineframework reactor project/);
});
