browser.runtime.onInstalled.addListener(function(details) {
  if (details && details.reason === 'install') {
    browser.tabs.create({
      url: browser.runtime.getURL('setup.html')
    });
  }
});

var LOCAL_PROXY_HOST = '127.0.0.1';
var LOCAL_PROXY_PORT = 7782;
var DAEMON_RPC_URL = 'http://127.0.0.1:7779';

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

function getPopupState(siteName) {
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
      manifest: manifest,
      known: known,
      pinned: Array.isArray(pinnedSites) && pinnedSites.indexOf(siteName) !== -1
    };
  });
}

browser.runtime.onMessage.addListener(function(message) {
  if (!message || !message.type) {
    return undefined;
  }

  if (message.type === 'getTrustIndicator') {
    return rpcRequest('get_site_manifest', { name: message.siteName }).then(function(response) {
      return response ? response.trust : null;
    }).catch(function() {
      return null;
    });
  }

  if (message.type === 'getPopupState') {
    return getPopupState(message.siteName);
  }

  if (message.type === 'trustSite') {
    return rpcRequest('trust_site', {
      name: message.siteName,
      pin: !!message.pin
    });
  }

  if (message.type === 'untrustSite') {
    return rpcRequest('untrust_site', {
      name: message.siteName,
      unpin: !!message.unpin
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
      urls: ['http://*.loom/*', 'http://*.loom', 'https://*.loom/*', 'https://*.loom'],
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
