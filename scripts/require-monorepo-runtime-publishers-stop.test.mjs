import assert from 'node:assert/strict';
import test from 'node:test';
import { assertMonorepoPublishersStopped } from './require-monorepo-runtime-publishers-stop.mjs';

const artifacts = [
  { artifactId: 'pipelineframework' },
  { artifactId: 'pipelineframework-deployment' },
  { artifactId: 'pipelineframework-runtime-spring' },
  { artifactId: 'persistence-plugin' },
  { artifactId: 'cache-plugin' },
  { artifactId: 'repository-plugin' },
];
const manifest = (reactorSourceMirror = true) => ({
  publicArtifacts: [],
  externalArtifacts: artifacts.map(({ artifactId }) => ({ artifactId, ownership: 'external', reactorSourceMirror })),
});
const mirrors = (deploySkip = 'true') => Object.fromEntries(
  artifacts.map(({ artifactId }) => [artifactId, `<project><properties><maven.deploy.skip>${deploySkip}</maven.deploy.skip></properties></project>`]),
);
const excludedFramework = `<project><profile><id>central-publishing</id><build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId><configuration><excludeArtifacts>${artifacts.map(({ artifactId }) => `<excludeArtifact>${artifactId}</excludeArtifact>`).join('')}</excludeArtifacts></configuration></plugin></plugins></build></profile></project>`;

test('accepts external, marked, non-deployable source mirrors excluded from Central', () => {
  assert.doesNotThrow(() => assertMonorepoPublishersStopped(manifest(), mirrors(), excludedFramework, artifacts));
});

test('accepts a source mirror removed after publisher handoff', () => {
  const sourceMirrors = mirrors();
  delete sourceMirrors.pipelineframework;
  const value = manifest();
  value.externalArtifacts[0].reactorSourceMirror = false;
  assert.doesNotThrow(() => assertMonorepoPublishersStopped(value, sourceMirrors, excludedFramework, artifacts));
});

test('rejects a public artifact', () => {
  const value = manifest();
  value.publicArtifacts.push({ artifactId: 'pipelineframework' });
  assert.throws(() => assertMonorepoPublishersStopped(value, mirrors(), excludedFramework, artifacts), /still declares pipelineframework as a public artifact/);
});

test('rejects missing external ownership', () => {
  const value = manifest();
  value.externalArtifacts = value.externalArtifacts.filter(({ artifactId }) => artifactId !== 'cache-plugin');
  assert.throws(() => assertMonorepoPublishersStopped(value, mirrors(), excludedFramework, artifacts), /does not declare cache-plugin as externally owned/);
});

test('rejects an unmarked source mirror', () => {
  assert.throws(() => assertMonorepoPublishersStopped(manifest(false), mirrors(), excludedFramework, artifacts), /source mirror is not marked reactorSourceMirror/);
});

test('rejects a deployable source mirror', () => {
  const sourceMirrors = mirrors();
  sourceMirrors['pipelineframework-deployment'] = sourceMirrors['pipelineframework-deployment'].replace('>true<', '>false<');
  assert.throws(() => assertMonorepoPublishersStopped(manifest(), sourceMirrors, excludedFramework, artifacts), /pipelineframework-deployment source mirror remains deployable/);
});

test('rejects a mirror included in the Central bundle', () => {
  const framework = excludedFramework.replace('<excludeArtifact>repository-plugin</excludeArtifact>', '');
  assert.throws(() => assertMonorepoPublishersStopped(manifest(), mirrors(), framework, artifacts), /Central bundle does not exclude the repository-plugin mirror/);
});

test('accepts a deploy plugin skip as an effective non-deployable mirror', () => {
  const sourceMirrors = mirrors();
  sourceMirrors['cache-plugin'] = '<project><build><plugins><plugin><artifactId>maven-deploy-plugin</artifactId><configuration><skip>true</skip></configuration></plugin></plugins></build></project>';
  assert.doesNotThrow(() => assertMonorepoPublishersStopped(manifest(), sourceMirrors, excludedFramework, artifacts));
});
