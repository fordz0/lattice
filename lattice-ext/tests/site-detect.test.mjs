import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const siteDetectPath = path.resolve(__dirname, '../site-detect.js');
const sitePagePath = path.resolve(__dirname, './fixtures/site-page-badge.js');

class FakeEventTarget {
  constructor() {
    this.listeners = new Map();
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  dispatchEvent(event) {
    const listeners = this.listeners.get(event.type) || [];
    for (const listener of listeners) {
      listener.call(this, event);
    }
  }
}

class FakeClassList {
  add() {}
  toggle() {}
}

class FakeElement extends FakeEventTarget {
  constructor(tagName, documentRef) {
    super();
    this.tagName = tagName;
    this.ownerDocument = documentRef;
    this.dataset = {};
    this.textContent = '';
    this.attributes = new Map();
    this.classList = new FakeClassList();
  }

  setAttribute(name, value) {
    this.attributes.set(name, value);
    if (name.startsWith('data-')) {
      this.dataset[dataAttributeToKey(name)] = value;
    }
    this.ownerDocument.notifyMutation(this, name);
  }

  getAttribute(name) {
    if (name.startsWith('data-')) {
      return this.dataset[dataAttributeToKey(name)] ?? null;
    }
    return this.attributes.get(name) ?? null;
  }
}

class FakeDocument extends FakeEventTarget {
  constructor({ includeBadge = true } = {}) {
    super();
    this.documentElement = new FakeElement('html', this);
    this.body = new FakeElement('body', this);
    this.readyState = 'complete';
    this.observers = [];
    this.nodesById = new Map();

    if (includeBadge) {
      this.nodesById.set('extension-badge', new FakeElement('div', this));
      this.nodesById.set('extension-badge-link', new FakeElement('a', this));
      this.nodesById.set('extension-badge-text', new FakeElement('span', this));
    }
  }

  getElementById(id) {
    return this.nodesById.get(id) || null;
  }

  querySelector(selector) {
    if (selector.startsWith('#')) {
      return this.getElementById(selector.slice(1));
    }
    return null;
  }

  querySelectorAll() {
    return [];
  }

  notifyMutation(target, attributeName) {
    for (const observer of this.observers) {
      const watchingTarget = observer.targets.has(target);
      const matchesAttribute =
        !observer.attributeFilter || observer.attributeFilter.includes(attributeName);
      if (watchingTarget && matchesAttribute && !observer.disconnected) {
        observer.callback([{ target, attributeName }]);
      }
    }
  }
}

class FakeMutationObserver {
  constructor(callback, documentRef) {
    this.callback = callback;
    this.documentRef = documentRef;
    this.targets = new Set();
    this.attributeFilter = null;
    this.disconnected = false;
    this.documentRef.observers.push(this);
  }

  observe(target, options = {}) {
    this.targets.add(target);
    this.attributeFilter = options.attributeFilter || null;
  }

  disconnect() {
    this.disconnected = true;
  }
}

function dataAttributeToKey(name) {
  return name
    .slice(5)
    .replace(/-([a-z])/g, (_, char) => char.toUpperCase());
}

function loadScript(scriptPath, { documentRef, locationPath = '/index.html' }) {
  const windowTarget = new FakeEventTarget();
  const timeouts = [];
  const messages = [];

  const context = vm.createContext({
    console,
    document: documentRef,
    location: { pathname: locationPath, origin: 'https://lattice.benjf.dev' },
    window: {
      addEventListener: windowTarget.addEventListener.bind(windowTarget),
      dispatchEvent: windowTarget.dispatchEvent.bind(windowTarget),
      postMessage(payload, origin) {
        messages.push({ payload, origin });
        windowTarget.dispatchEvent({
          type: 'message',
          source: this,
          data: payload,
        });
      },
      setTimeout(fn) {
        timeouts.push(fn);
        return timeouts.length;
      },
      location: { origin: 'https://lattice.benjf.dev' },
    },
    CustomEvent: class CustomEvent {
      constructor(type) {
        this.type = type;
      }
    },
    MutationObserver: class extends FakeMutationObserver {
      constructor(callback) {
        super(callback, documentRef);
      }
    },
    IntersectionObserver: class {
      observe() {}
      disconnect() {}
    },
  });

  const source = fs.readFileSync(scriptPath, 'utf8');
  vm.runInContext(source, context, { filename: scriptPath });

  return {
    fireDOMContentLoaded() {
      documentRef.dispatchEvent({ type: 'DOMContentLoaded' });
    },
    runTimeouts() {
      while (timeouts.length) {
        timeouts.shift()();
      }
    },
    messages,
  };
}

test('site-detect updates the badge when the badge html exists', () => {
  const documentRef = new FakeDocument({ includeBadge: true });
  const env = loadScript(siteDetectPath, { documentRef });

  const badge = documentRef.getElementById('extension-badge');
  const badgeText = documentRef.getElementById('extension-badge-text');

  assert.equal(documentRef.documentElement.dataset.latticeExtension, 'installed');
  assert.equal(documentRef.body.dataset.latticeExtension, 'installed');
  assert.equal(badge.dataset.extensionState, 'installed');
  assert.equal(
    badgeText.textContent,
    'Connected to the Lattice network',
  );
  assert.equal(env.messages.length, 1);
  assert.equal(env.messages[0].origin, 'https://lattice.benjf.dev');
  assert.equal(env.messages[0].payload.type, 'lattice-extension-ready');
});

test('site-detect does not throw when the badge html is absent', () => {
  const documentRef = new FakeDocument({ includeBadge: false });
  assert.doesNotThrow(() => loadScript(siteDetectPath, { documentRef }));
  assert.equal(documentRef.documentElement.dataset.latticeExtension, 'installed');
  assert.equal(documentRef.body.dataset.latticeExtension, 'installed');
});

test('site page badge flips to installed when the extension marker appears later', () => {
  const documentRef = new FakeDocument({ includeBadge: true });
  const env = loadScript(sitePagePath, { documentRef });
  env.fireDOMContentLoaded();

  const badge = documentRef.getElementById('extension-badge');
  const badgeText = documentRef.getElementById('extension-badge-text');
  assert.equal(badge.dataset.extensionState, 'missing');
  assert.equal(
    badgeText.textContent,
    'Install the Firefox extension to browse .loom sites',
  );

  documentRef.documentElement.setAttribute('data-lattice-extension', 'installed');
  assert.equal(badge.dataset.extensionState, 'installed');
  assert.equal(
    badgeText.textContent,
    'Connected to the Lattice network',
  );
});

test('site page script does not throw when the badge html is absent', () => {
  const documentRef = new FakeDocument({ includeBadge: false });
  const env = loadScript(sitePagePath, { documentRef });
  assert.doesNotThrow(() => env.fireDOMContentLoaded());
  assert.doesNotThrow(() => env.runTimeouts());
});
