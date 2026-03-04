browser.runtime.onInstalled.addListener(function(details) {
  if (details && details.reason === 'install') {
    browser.tabs.create({
      url: browser.runtime.getURL('setup.html')
    });
  }
});

browser.webRequest.onBeforeRequest.addListener(
  function(requestInfo) {
    try {
      var url = new URL(requestInfo.url);
      if (url.protocol === 'https:' && url.hostname && url.hostname.endsWith('.lat')) {
        url.protocol = 'http:';
        return { redirectUrl: url.toString() };
      }
    } catch (_e) {
      // Ignore parse errors and continue.
    }

    return {};
  },
  { urls: ['https://*.lat/*', 'https://*.lat'], types: ['main_frame', 'sub_frame'] },
  ['blocking']
);

browser.proxy.onRequest.addListener(
  function(requestInfo) {
    try {
      var url = new URL(requestInfo.url);
      if (url.protocol === 'http:' && url.hostname && url.hostname.endsWith('.lat')) {
        return {
          type: 'http',
          host: '127.0.0.1',
          port: 7781
        };
      }
    } catch (_e) {
      // Ignore URL parse errors and fall back to direct.
    }

    return { type: 'direct' };
  },
  { urls: ['<all_urls>'] }
);
