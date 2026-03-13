import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const helpers = require('../setup-helpers.js');

test('parseGithubReleaseVersion strips lattice tag prefixes', () => {
  assert.equal(helpers.parseGithubReleaseVersion('lattice-v0.1.1'), '0.1.1');
  assert.equal(helpers.parseGithubReleaseVersion('v2.0.0'), '2.0.0');
});

test('compareVersions orders dotted versions', () => {
  assert.equal(helpers.compareVersions('0.1.5', '0.1.4'), 1);
  assert.equal(helpers.compareVersions('0.1.5', '0.1.5'), 0);
  assert.equal(helpers.compareVersions('0.1.4', '0.1.5'), -1);
  assert.equal(helpers.compareVersions('0.2', '0.1.9'), 1);
});
