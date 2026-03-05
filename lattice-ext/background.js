browser.runtime.onInstalled.addListener(function(details) {
  if (details && details.reason === 'install') {
    browser.tabs.create({
      url: browser.runtime.getURL('setup.html')
    });
  }
});

var LOCAL_PROXY_HOST = '127.0.0.1';
var LOCAL_PROXY_PORT = 7782;

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
