browser.runtime.onInstalled.addListener(function(details) {
  if (details && details.reason === 'install') {
    browser.tabs.create({
      url: browser.runtime.getURL('setup.html')
    });
  }
});

var LOCAL_PROXY_HOST = '127.0.0.1';
var LOCAL_PROXY_PORT = 7782;
var DAEMON_RPC_URL = 'http://127.0.0.1:7780';
var hiddenOverlayByTab = {};

function isLoomHost(hostname) {
  if (!hostname || !hostname.endsWith('.loom')) {
    return false;
  }

  var site = hostname.slice(0, -'.loom'.length);
  if (!site || site.indexOf('.') !== -1) {
    return false;
  }

  return true;
}

function getSiteName(hostname) {
  if (!isLoomHost(hostname)) {
    return null;
  }

  return hostname.slice(0, -'.loom'.length);
}

function siteNameFromUrl(urlString) {
  try {
    return getSiteName(new URL(urlString).hostname);
  } catch (_e) {
    return null;
  }
}

function rpcRequest(method, params) {
  return fetch(DAEMON_RPC_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: String(Date.now()) + ':' + method,
      method: method,
      params: params || {}
    })
  }).then(function(response) {
    if (!response.ok) {
      throw new Error('daemon rpc http ' + response.status);
    }
    return response.json();
  }).then(function(payload) {
    if (payload.error) {
      throw new Error(payload.error.message || 'daemon rpc error');
    }
    return payload.result;
  });
}

function getSiteState(siteName) {
  return Promise.all([
    rpcRequest('get_site_manifest', { name: siteName }).catch(function() {
      return null;
    }),
    rpcRequest('known_publisher_status', { name: siteName }).catch(function() {
      return null;
    }),
    rpcRequest('list_pinned', {}).catch(function() {
      return [];
    })
  ]).then(function(results) {
    var manifest = results[0];
    var known = results[1];
    var pinnedSites = results[2] || [];
    return {
      siteName: siteName,
      manifest: manifest,
      known: known,
      pinned: Array.isArray(pinnedSites) && pinnedSites.indexOf(siteName) !== -1
    };
  });
}

function isOverlayHidden(tabId, siteName) {
  if (typeof tabId !== 'number' || !siteName) {
    return false;
  }
  return hiddenOverlayByTab[tabId] === siteName;
}

function setOverlayHidden(tabId, siteName, hidden) {
  if (typeof tabId !== 'number' || !siteName) {
    return;
  }
  if (hidden) {
    hiddenOverlayByTab[tabId] = siteName;
  } else if (hiddenOverlayByTab[tabId] === siteName) {
    delete hiddenOverlayByTab[tabId];
  }
}

function isExplicitlyTrusted(state) {
  return !!(state && state.known && state.known.explicitly_trusted);
}

function trustStatus(state) {
  if (!state || !state.manifest || !state.manifest.trust) {
    return 'offline';
  }
  return state.manifest.trust.status || 'offline';
}

function badgeStyleForState(state) {
  var status = trustStatus(state);
  if (status === 'key_changed') {
    return {
      text: '!',
      color: '#c9811a',
      title: 'Publisher key changed'
    };
  }
  if (isExplicitlyTrusted(state)) {
    return {
      text: 'OK',
      color: '#1f8f4e',
      title: 'Trusted and cached'
    };
  }
  if (status === 'matches') {
    return {
      text: '·',
      color: '#5c6b78',
      title: 'Known publisher'
    };
  }
  if (status === 'first_seen') {
    return {
      text: '+',
      color: '#4a6278',
      title: 'First visit'
    };
  }
  return {
    text: '',
    color: '#4a6278',
    title: 'Lattice'
  };
}

function sendMessageToTab(tabId, payload) {
  if (!browser.tabs || typeof browser.tabs.sendMessage !== 'function') {
    return Promise.resolve();
  }
  return browser.tabs.sendMessage(tabId, payload).catch(function() {
    return null;
  });
}

function sendOverlayState(tabId, siteName, state) {
  if (typeof tabId !== 'number') {
    return Promise.resolve();
  }
  return sendMessageToTab(tabId, {
    type: 'trustStateChanged',
    siteName: siteName,
    state: state
  });
}

function broadcastSiteState(siteName, state) {
  if (!browser.tabs || typeof browser.tabs.query !== 'function') {
    return Promise.resolve();
  }
  return browser.tabs.query({}).then(function(tabs) {
    return Promise.all((tabs || []).map(function(tab) {
      if (!tab || !tab.id) {
        return null;
      }
      if (siteNameFromUrl(tab.url) !== siteName) {
        return null;
      }
      return sendMessageToTab(tab.id, {
        type: 'trustStateChanged',
        siteName: siteName,
        state: state
      });
    }));
  });
}

function updateBrowserActionForTab(tab) {
  if (!browser.browserAction || !tab || !tab.id) {
    return Promise.resolve();
  }

  var siteName = siteNameFromUrl(tab.url);
  if (!siteName) {
    return Promise.all([
      browser.browserAction.setBadgeText({ tabId: tab.id, text: '' }),
      browser.browserAction.setTitle({ tabId: tab.id, title: 'Lattice' })
    ]);
  }

  return getSiteState(siteName).then(function(state) {
    var style = badgeStyleForState(state);
    return Promise.all([
      browser.browserAction.setBadgeText({ tabId: tab.id, text: style.text }),
      browser.browserAction.setBadgeBackgroundColor({ tabId: tab.id, color: style.color }),
      browser.browserAction.setTitle({
        tabId: tab.id,
        title: siteName + '.loom — ' + style.title
      })
    ]);
  }).catch(function() {
    return Promise.all([
      browser.browserAction.setBadgeText({ tabId: tab.id, text: '?' }),
      browser.browserAction.setBadgeBackgroundColor({ tabId: tab.id, color: '#8b1e00' }),
      browser.browserAction.setTitle({ tabId: tab.id, title: siteName + '.loom — daemon unavailable' })
    ]);
  });
}

function refreshActiveTabUI() {
  if (!browser.tabs || typeof browser.tabs.query !== 'function') {
    return Promise.resolve();
  }
  return browser.tabs.query({ active: true, currentWindow: true }).then(function(tabs) {
    if (!tabs || !tabs[0]) {
      return null;
    }
    return updateBrowserActionForTab(tabs[0]);
  });
}

function showOverlayForTab(tab) {
  if (!tab || !tab.id) {
    return Promise.resolve();
  }
  var siteName = siteNameFromUrl(tab.url);
  if (!siteName) {
    return Promise.resolve();
  }
  setOverlayHidden(tab.id, siteName, false);
  return getSiteState(siteName).then(function(state) {
    var nextState = Object.assign({}, state, { overlayHidden: false });
    return sendOverlayState(tab.id, siteName, nextState);
  }).catch(function() {
    return null;
  });
}

function applyTrustChange(siteName, trust, pinOrUnpin) {
  var method = trust ? 'trust_site' : 'untrust_site';
  var params = trust
    ? { name: siteName, pin: !!pinOrUnpin }
    : { name: siteName, unpin: !!pinOrUnpin };

  return rpcRequest(method, params).then(function(result) {
    return getSiteState(siteName).then(function(state) {
      return broadcastSiteState(siteName, state).then(function() {
        return refreshActiveTabUI().then(function() {
          return {
            status: (result && result.status) || 'ok',
            state: state
          };
        });
      });
    });
  });
}

if (browser.tabs) {
  if (browser.tabs.onActivated) {
    browser.tabs.onActivated.addListener(function(activeInfo) {
      if (!browser.tabs.get) {
        return;
      }
      browser.tabs.get(activeInfo.tabId).then(updateBrowserActionForTab).catch(function() {
        return null;
      });
    });
  }

  if (browser.tabs.onUpdated) {
    browser.tabs.onUpdated.addListener(function(tabId, changeInfo, tab) {
      if (changeInfo.url) {
        delete hiddenOverlayByTab[tabId];
      }
      if (changeInfo.status === 'complete' || changeInfo.url) {
        updateBrowserActionForTab(tab);
      }
    });
  }
}

if (browser.browserAction && browser.browserAction.onClicked) {
  browser.browserAction.onClicked.addListener(function(tab) {
    showOverlayForTab(tab);
  });
}

browser.runtime.onMessage.addListener(function(message, sender) {
  if (!message || !message.type) {
    return undefined;
  }
  var tabId = typeof message.tabId === 'number'
    ? message.tabId
    : (sender && sender.tab && typeof sender.tab.id === 'number' ? sender.tab.id : null);

  if (message.type === 'getTrustIndicator' || message.type === 'getSiteState') {
    return getSiteState(message.siteName).then(function(state) {
      if (state) {
        state.overlayHidden = isOverlayHidden(tabId, message.siteName);
      }
      return state;
    }).catch(function() {
      return null;
    });
  }

  if (message.type === 'getPopupState') {
    return getSiteState(message.siteName).then(function(state) {
      if (state) {
        state.overlayHidden = isOverlayHidden(tabId, message.siteName);
      }
      return state;
    });
  }

  if (message.type === 'trustSite') {
    return applyTrustChange(message.siteName, true, !!message.pin);
  }

  if (message.type === 'untrustSite') {
    return applyTrustChange(message.siteName, false, !!message.unpin);
  }

  if (message.type === 'hideOverlay') {
    setOverlayHidden(tabId, message.siteName, true);
    return Promise.resolve({ status: 'ok' });
  }

  if (message.type === 'showOverlay') {
    setOverlayHidden(tabId, message.siteName, false);
    return getSiteState(message.siteName).then(function(state) {
      if (state) {
        state.overlayHidden = false;
      }
      return sendOverlayState(tabId, message.siteName, state).then(function() {
        return { status: 'ok', state: state };
      });
    }).catch(function() {
      return { status: 'ok', state: null };
    });
  }

  return undefined;
});

// Upgrade plain HTTP .loom navigation to HTTPS while keeping the .loom hostname in the URL bar.
if (typeof browser.webRequest !== 'undefined') {
  browser.webRequest.onBeforeRequest.addListener(
    function(requestInfo) {
      try {
        var url = new URL(requestInfo.url);
        if (url.protocol === 'http:' && isLoomHost(url.hostname)) {
          url.protocol = 'https:';
          return { redirectUrl: url.toString() };
        }
      } catch (_e) {
        // Ignore parse errors and continue.
      }

      return {};
    },
    {
      urls: ['http://*.loom/*', 'https://*.loom/*'],
      types: ['main_frame', 'sub_frame']
    },
    ['blocking']
  );
}

// Route all .loom HTTP(S) requests through the local Lattice proxy.
if (typeof browser.proxy !== 'undefined' && browser.proxy.onRequest) {
  browser.proxy.onRequest.addListener(
    function(requestInfo) {
      try {
        var url = new URL(requestInfo.url);
        if ((url.protocol === 'http:' || url.protocol === 'https:') && isLoomHost(url.hostname)) {
          return {
            type: 'http',
            host: LOCAL_PROXY_HOST,
            port: LOCAL_PROXY_PORT
          };
        }
      } catch (_e) {
        // Ignore parse errors and fall back to direct.
      }
      return { type: 'direct' };
    },
    { urls: ['<all_urls>'] }
  );
}

refreshActiveTabUI();
