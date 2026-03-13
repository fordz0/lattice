const prefKey = document.getElementById('pref-key').textContent.trim();

var latticeConfigApi = typeof LatticeConfig !== 'undefined' && LatticeConfig.defaults
  ? LatticeConfig
  : {
      defaults: function() {
        return {
          localHost: '127.0.0.1',
          rpcPort: 7780,
          httpPort: 7781,
          proxyPort: 7782
        };
      },
      caCertUrl: function(config) {
        return 'http://' + config.localHost + ':' + config.httpPort + '/__lattice_ca.pem';
      }
    };

document.getElementById('copy-pref').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(prefKey);
  } catch (_err) {
    const area = document.createElement('textarea');
    area.value = prefKey;
    document.body.appendChild(area);
    area.select();
    document.execCommand('copy');
    document.body.removeChild(area);
  }
});

document.getElementById('open-config').addEventListener('click', () => {
  browser.tabs.create({ url: 'about:config' });
});

document.getElementById('download-ca').addEventListener('click', () => {
  browser.tabs.create({ url: latticeConfigApi.caCertUrl(latticeConfigApi.defaults()) });
});

document.getElementById('open-certs').addEventListener('click', () => {
  browser.tabs.create({ url: 'about:preferences#privacy' });
});

document.getElementById('done-test').addEventListener('click', async () => {
  const tabs = await browser.tabs.query({ active: true, currentWindow: true });
  if (tabs.length > 0 && tabs[0].id) {
    browser.tabs.update(tabs[0].id, { url: 'https://benjf.loom' });
  } else {
    browser.tabs.create({ url: 'https://benjf.loom' });
  }
});
