#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const expectedDeployability = new Map([
  ['pipelineframework-runtime-parent', true],
  ['pipelineframework', true],
  ['pipelineframework-deployment', true],
  ['pipelineframework-runtime-spring', true],
  ['pipelineframework-release-maven-plugin', true],
  ['persistence-plugin', true],
  ['cache-plugin', true],
  ['repository-plugin', true],
  ['framework-plugins', false],
  ['framework-foundational-plugins', false],
  ['pipelineframework-spring-smoke-tests', false],
  ['pipelineframework-spring-blocking-smoke-tests', false],
]);

function elementValue(xml, element) {
  return xml.match(new RegExp(`<${element}>([^<]+)</${element}>`))?.[1]?.trim() ?? '';
}

export function assertEffectivePublication(effectivePom, expected = expectedDeployability) {
  const projects = new Map();
  for (const match of effectivePom.matchAll(/<project(?:\s[^>]*)?>([\s\S]*?)<\/project>/g)) {
    const project = match[1].replace(/<parent>[\s\S]*?<\/parent>/, '');
    if (elementValue(project, 'groupId') !== 'org.pipelineframework') continue;
    const artifactId = elementValue(project, 'artifactId');
    if (!artifactId) continue;
    if (projects.has(artifactId)) throw new Error(`effective POM contains duplicate project: ${artifactId}`);
    const properties = project.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
    const deploySkip = elementValue(properties, 'maven.deploy.skip');
    if (deploySkip !== 'true' && deploySkip !== 'false') {
      throw new Error(`effective maven.deploy.skip for ${artifactId} is not boolean: ${deploySkip || '<unset>'}`);
    }
    const central = project.match(/<artifactId>central-publishing-maven-plugin<\/artifactId>([\s\S]*?)(?=<plugin>|<\/plugins>)/g) ?? [];
    projects.set(artifactId, {
      deployable: deploySkip === 'false',
      centralPresent: central.length > 0,
      centralSkipped: central.some((plugin) => /<skipPublishing>\s*true\s*<\/skipPublishing>/.test(plugin)),
    });
  }

  for (const [artifactId, deployable] of expected) {
    const project = projects.get(artifactId);
    if (!project) throw new Error(`expected reactor project is missing from effective POM: ${artifactId}`);
    if (project.deployable !== deployable) {
      throw new Error(`effective deployability drift for ${artifactId}: expected ${deployable}, found ${project.deployable}`);
    }
    if (!project.centralPresent) throw new Error(`Central plugin is missing for ${artifactId}`);
    if (!deployable && !project.centralSkipped) {
      throw new Error(`internal artifact is not skipped by the Central plugin: ${artifactId}`);
    }
    if (deployable && project.centralSkipped) {
      throw new Error(`public artifact is skipped by the Central plugin: ${artifactId}`);
    }
  }

  for (const artifactId of projects.keys()) {
    if (!expected.has(artifactId)) throw new Error(`unexpected org.pipelineframework reactor project: ${artifactId}`);
  }
  return projects;
}

function main() {
  const effectivePomPath = process.argv[2];
  if (!effectivePomPath) throw new Error('Usage: verify-runtime-publication.mjs <effective-pom.xml>');
  const projects = assertEffectivePublication(fs.readFileSync(path.resolve(effectivePomPath), 'utf8'));
  console.log(`verified runtime publication flags for ${projects.size} reactor projects`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
