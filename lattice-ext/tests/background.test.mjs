import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const backgroundPath = path.resolve(__dirname, '../background.js');

function loadBackgroundWithFetch(fetchImpl) {
  const listeners = {
    installed: null,
    message: null,
    redirect: null,
    proxy: null,
    browserActionClicked: null,
    tabActivated: null,
    tabUpdated: null,
  };

  const browser = {
    runtime: {
      getURL(value) {
        return 'moz-extension://test/' + value;
      },
      onInstalled: {
        addListener(fn) {
          listeners.installed = fn;
        },
      },
      onMessage: {
        addListener(fn) {
          listeners.message = fn;
        },
      },
    },
    tabs: {
      create() {},
      query() {
        return Promise.resolve([]);
      },
      sendMessage() {
        return Promise.resolve();
      },
      get() {
        return Promise.resolve({ id: 1, url: 'https://lattice.loom/' });
      },
      onActivated: {
        addListener(fn) {
          listeners.tabActivated = fn;
        },
      },
      onUpdated: {
        addListener(fn) {
          listeners.tabUpdated = fn;
        },
      },
    },
    webRequest: {
      onBeforeRequest: {
        addListener(fn) {
          listeners.redirect = fn;
        },
      },
    },
    proxy: {
      onRequest: {
        addListener(fn) {
          listeners.proxy = fn;
        },
      },
    },
    browserAction: {
      setBadgeText() {
        return Promise.resolve();
      },
      setBadgeBackgroundColor() {
        return Promise.resolve();
      },
      setTitle() {
        return Promise.resolve();
      },
      onClicked: {
        addListener(fn) {
          listeners.browserActionClicked = fn;
        },
      },
    },
  };

  const context = vm.createContext({
    URL,
    browser,
    console,
    fetch: fetchImpl,
  });

  const source = fs.readFileSync(backgroundPath, 'utf8');
  vm.runInContext(source, context, { filename: backgroundPath });
  return { listeners, browser };
}

function loadBackground() {
  return loadBackgroundWithFetch(function() {
    throw new Error('unexpected fetch in background smoke test');
  });
}

function okJson(result) {
  return {
    ok: true,
    json() {
      return Promise.resolve({ result });
    },
  };
}

function plain(value) {
  return JSON.parse(JSON.stringify(value));
}

test('background registers loom request listeners', () => {
  const { listeners } = loadBackground();
  assert.equal(typeof listeners.installed, 'function');
  assert.equal(typeof listeners.message, 'function');
  assert.equal(typeof listeners.redirect, 'function');
  assert.equal(typeof listeners.proxy, 'function');
  assert.equal(typeof listeners.browserActionClicked, 'function');
});

test('background upgrades plain http loom requests to https', () => {
  const { listeners } = loadBackground();
  const redirect = listeners.redirect;

  assert.deepEqual(
    plain(redirect({ url: 'http://lattice.loom/' })),
    { redirectUrl: 'https://lattice.loom/' },
  );
  assert.deepEqual(plain(redirect({ url: 'https://lattice.loom/' })), {});
  assert.deepEqual(plain(redirect({ url: 'https://example.com/' })), {});
});

test('background routes loom hosts through the local proxy only', () => {
  const { listeners } = loadBackground();
  const proxy = listeners.proxy;

  assert.deepEqual(plain(proxy({ url: 'https://lattice.loom/' })), {
    type: 'http',
    host: '127.0.0.1',
    port: 7782,
  });
  assert.deepEqual(plain(proxy({ url: 'http://fray.loom/thread' })), {
    type: 'http',
    host: '127.0.0.1',
    port: 7782,
  });
  assert.deepEqual(plain(proxy({ url: 'https://bad.name.loom/' })), { type: 'direct' });
  assert.deepEqual(plain(proxy({ url: 'https://example.com/' })), { type: 'direct' });
});

test('background talks to the daemon on the RPC port', async () => {
  const urls = [];
  const { listeners } = loadBackgroundWithFetch(function(url, options) {
    urls.push(url);
    const request = JSON.parse(options.body);
    if (request.method === 'get_site_manifest') {
      return Promise.resolve(okJson({ manifest_json: '{}', trust: { status: 'first_seen', explicitly_trusted: false, first_seen_at: null, previous_key: null } }));
    }
    if (request.method === 'known_publisher_status') {
      return Promise.resolve(okJson({ site_name: 'lattice', publisher_b64: 'abc', first_seen_at: 1, explicitly_trusted: false, explicitly_trusted_at: null }));
    }
    if (request.method === 'list_pinned') {
      return Promise.resolve(okJson([]));
    }
    return Promise.resolve(okJson(null));
  });

  await listeners.message({ type: 'getPopupState', siteName: 'lattice' });
  assert.deepEqual(urls, [
    'http://127.0.0.1:7780',
    'http://127.0.0.1:7780',
    'http://127.0.0.1:7780',
  ]);
});

test('browser action click re-shows the inline overlay for loom tabs', async () => {
  const sent = [];
  const { listeners, browser } = loadBackgroundWithFetch(function(_url, options) {
    const request = JSON.parse(options.body);
    if (request.method === 'get_site_manifest') {
      return Promise.resolve(okJson({
        manifest_json: '{}',
        trust: {
          status: 'matches',
          explicitly_trusted: false,
          first_seen_at: 1,
          previous_key: null,
        },
      }));
    }
    if (request.method === 'known_publisher_status') {
      return Promise.resolve(okJson({
        site_name: 'lattice',
        publisher_b64: 'abc',
        first_seen_at: 1,
        explicitly_trusted: false,
        explicitly_trusted_at: null,
      }));
    }
    if (request.method === 'list_pinned') {
      return Promise.resolve(okJson([]));
    }
    return Promise.resolve(okJson(null));
  });

  browser.tabs.sendMessage = function(tabId, payload) {
    sent.push({ tabId, payload });
    return Promise.resolve();
  };

  await listeners.browserActionClicked({ id: 7, url: 'https://lattice.loom/' });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(sent.length, 1);
  assert.equal(sent[0].tabId, 7);
  assert.equal(sent[0].payload.type, 'trustStateChanged');
  assert.equal(sent[0].payload.siteName, 'lattice');
  assert.equal(sent[0].payload.state.overlayHidden, false);
});
