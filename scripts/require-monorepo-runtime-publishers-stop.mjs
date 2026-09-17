#!/usr/bin/env node
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const publishedArtifacts = [
  { artifactId: 'pipelineframework', mirrorPath: 'framework/runtime/pom.xml' },
  { artifactId: 'pipelineframework-deployment', mirrorPath: 'framework/deployment/pom.xml' },
  { artifactId: 'pipelineframework-runtime-spring', mirrorPath: 'framework/runtime-spring/pom.xml' },
  { artifactId: 'persistence-plugin', mirrorPath: 'framework/plugins/foundational/persistence/pom.xml' },
  { artifactId: 'cache-plugin', mirrorPath: 'framework/plugins/foundational/cache/pom.xml' },
  { artifactId: 'repository-plugin', mirrorPath: 'framework/plugins/foundational/repository/pom.xml' },
];

const repository = 'The-Pipeline-Framework/pipelineframework';
const apiRoot = `https://api.github.com/repos/${repository}`;

function externalArtifact(manifest, artifactId) {
  return manifest.externalArtifacts.find((entry) => entry?.artifactId === artifactId);
}

function centralPublishingPlugin(frameworkPom) {
  const profile = [...(frameworkPom ?? '').matchAll(/<profile>([\s\S]*?)<\/profile>/g)]
    .map((match) => match[1])
    .find((candidate) => /<id>\s*central-publishing\s*<\/id>/.test(candidate));
  return [...(profile ?? '').matchAll(/<plugin>([\s\S]*?)<\/plugin>/g)]
    .map((match) => match[1])
    .find((plugin) => /<artifactId>\s*central-publishing-maven-plugin\s*<\/artifactId>/.test(plugin));
}

function centralExcludes(frameworkPom, artifactId) {
  const plugin = centralPublishingPlugin(frameworkPom);
  const exclusions = plugin?.match(/<excludeArtifacts>([\s\S]*?)<\/excludeArtifacts>/)?.[1] ?? '';
  return [...exclusions.matchAll(/<excludeArtifact>\s*([^<]+?)\s*<\/excludeArtifact>/g)]
    .some((match) => match[1] === artifactId);
}

function isNonDeployable(mirrorPom) {
  const properties = mirrorPom.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
  if (/<maven\.deploy\.skip>\s*true\s*<\/maven\.deploy\.skip>/.test(properties)) return true;

  const deployPlugin = [...mirrorPom.matchAll(/<plugin>([\s\S]*?)<\/plugin>/g)]
    .map((match) => match[1])
    .find((plugin) => /<artifactId>\s*maven-deploy-plugin\s*<\/artifactId>/.test(plugin));
  return /<skip>\s*true\s*<\/skip>/.test(deployPlugin ?? '');
}

export function assertMonorepoPublishersStopped(manifest, mirrors, frameworkPom, artifacts = publishedArtifacts) {
  if (!Array.isArray(manifest?.publicArtifacts) || !Array.isArray(manifest?.externalArtifacts)) {
    throw new Error('Monorepo publication manifest is missing public or external artifacts');
  }

  for (const { artifactId } of artifacts) {
    if (manifest.publicArtifacts.some((entry) => entry?.artifactId === artifactId)) {
      throw new Error(`Monorepo still declares ${artifactId} as a public artifact`);
    }

    const external = externalArtifact(manifest, artifactId);
    if (external?.ownership !== 'external') {
      throw new Error(`Monorepo does not declare ${artifactId} as externally owned`);
    }

    const mirrorPom = mirrors?.[artifactId];
    if (mirrorPom === undefined) continue;

    if (external.reactorSourceMirror !== true) {
      throw new Error(`Monorepo ${artifactId} source mirror is not marked reactorSourceMirror`);
    }
    if (!isNonDeployable(mirrorPom)) {
      throw new Error(`Monorepo ${artifactId} source mirror remains deployable`);
    }
    if (!centralExcludes(frameworkPom, artifactId)) {
      throw new Error(`Monorepo Central bundle does not exclude the ${artifactId} mirror`);
    }
  }
}

async function githubJson(url) {
  const headers = { Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const response = await fetch(url, { headers });
  if (!response.ok) throw new Error(`GitHub publication preflight failed: HTTP ${response.status} for ${url}`);
  return response.json();
}

async function githubFile(filePath, commitSha, optional = false) {
  const url = `${apiRoot}/contents/${filePath}?ref=${commitSha}`;
  const headers = { Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const response = await fetch(url, { headers });
  if (optional && response.status === 404) return undefined;
  if (!response.ok) throw new Error(`GitHub publication preflight failed: HTTP ${response.status} for ${filePath}`);
  const file = await response.json();
  if (file.type !== 'file' || file.encoding !== 'base64' || typeof file.content !== 'string') {
    throw new Error(`GitHub publication preflight returned invalid content for ${filePath}`);
  }
  return Buffer.from(file.content, 'base64').toString('utf8');
}

async function main() {
  const mainRef = await githubJson(`${apiRoot}/git/ref/heads/main`);
  const commitSha = mainRef.object?.sha;
  if (typeof commitSha !== 'string' || !/^[0-9a-f]{40}$/.test(commitSha)) {
    throw new Error('GitHub publication preflight returned no monorepo main commit');
  }

  const [manifestJson, frameworkPom, ...mirrorPoms] = await Promise.all([
    githubFile('framework/public-artifacts.json', commitSha),
    githubFile('framework/pom.xml', commitSha),
    ...publishedArtifacts.map(({ mirrorPath }) => githubFile(mirrorPath, commitSha, true)),
  ]);
  const mirrors = Object.fromEntries(publishedArtifacts.map(({ artifactId }, index) => [artifactId, mirrorPoms[index]]));
  assertMonorepoPublishersStopped(JSON.parse(manifestJson), mirrors, frameworkPom);
  console.log(`Monorepo ${commitSha} no longer publishes staged runtime artifacts; runtime is the sole publisher`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
