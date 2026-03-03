browser.runtime.onInstalled.addListener(function(details) {
  if (details && details.reason === 'install') {
    browser.tabs.create({
      url: browser.runtime.getURL('setup.html')
    });
  }
});

browser.proxy.onRequest.addListener(
  function(requestInfo) {
    try {
      var url = new URL(requestInfo.url);
      if (url.hostname && url.hostname.endsWith('.lat')) {
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
