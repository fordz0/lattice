const prefKey = document.getElementById('pref-key').textContent.trim();

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
  browser.tabs.create({ url: 'http://127.0.0.1:7781/__lattice_ca.pem' });
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
