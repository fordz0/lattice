const prefKey = document.getElementById('pref-key').textContent.trim();
const amoUrl = 'https://addons.mozilla.org/en-US/firefox/addon/lattice/';
const latestExtensionReleaseApi = 'https://api.github.com/repos/fordz0/lattice/releases?per_page=20';
const latestExtensionReleaseFallback = 'https://github.com/fordz0/lattice/releases?q=lattice-ext-v&expanded=true';

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

var latticeSetupHelpers = typeof LatticeSetupHelpers !== 'undefined'
  ? LatticeSetupHelpers
  : {
      parseGithubReleaseVersion: function(tagName) {
        return String(tagName || '')
          .replace(/^lattice-ext-v/i, '')
          .replace(/^lattice-v/i, '')
          .replace(/^v/i, '')
          .trim();
      },
      compareVersions: function(left, right) {
        const parse = function(value) {
          return String(value || '')
            .split(/[^0-9]+/)
            .filter(Boolean)
            .map(function(part) { return parseInt(part, 10) || 0; });
        };
        const a = parse(left);
        const b = parse(right);
        const len = Math.max(a.length, b.length);
        for (let index = 0; index < len; index += 1) {
          const av = a[index] || 0;
          const bv = b[index] || 0;
          if (av > bv) return 1;
          if (av < bv) return -1;
        }
        return 0;
      }
    };

function setReleaseNotice(copy, htmlUrl) {
  document.getElementById('release-copy').textContent = copy;
  const notice = document.getElementById('release-notice');
  notice.hidden = false;
  const button = document.getElementById('view-release');
  button.onclick = function() {
    browser.tabs.create({ url: htmlUrl });
  };
}

function setSetupNotice(title, copy) {
  const notice = document.getElementById('setup-notice');
  document.getElementById('setup-notice-title').textContent = title;
  document.getElementById('setup-notice-copy').textContent = copy;
  notice.hidden = false;
}

async function copyText(value) {
  try {
    await navigator.clipboard.writeText(value);
  } catch (_err) {
    const area = document.createElement('textarea');
    area.value = value;
    document.body.appendChild(area);
    area.select();
    document.execCommand('copy');
    document.body.removeChild(area);
  }
}

async function openFirefoxInternalPage(url, title, fallbackCopy) {
  try {
    await browser.tabs.create({ url: url });
  } catch (_err) {
    await copyText(url);
    setSetupNotice(
      title,
      fallbackCopy + ' We copied ' + url + ' to your clipboard so you can paste it into the Firefox address bar.'
    );
  }
}

async function checkForLatestRelease(showUpToDateMessage) {
  try {
    const response = await fetch(latestExtensionReleaseApi, {
      headers: { Accept: 'application/vnd.github+json' }
    });
    if (!response.ok) {
      throw new Error('GitHub returned HTTP ' + response.status);
    }
    const releases = await response.json();
    const release = releases.find(function(candidate) {
      return candidate.tag_name && /^lattice-ext-v/i.test(candidate.tag_name);
    });
    if (!release) {
      throw new Error('No lattice-ext release found');
    }
    const manifestVersion = browser.runtime.getManifest().version;
    const releaseVersion = latticeSetupHelpers.parseGithubReleaseVersion(release.tag_name);
    if (latticeSetupHelpers.compareVersions(releaseVersion, manifestVersion) > 0) {
      setReleaseNotice(
        'Version ' + releaseVersion + ' is available on GitHub. You are running extension version ' + manifestVersion + '.',
        release.html_url || latestExtensionReleaseFallback
      );
      return;
    }
    if (showUpToDateMessage) {
      setReleaseNotice(
        'You are already on the newest release we could find (' + manifestVersion + ').',
        release.html_url || latestExtensionReleaseFallback
      );
    }
  } catch (_err) {
    if (showUpToDateMessage) {
      setReleaseNotice(
        'We could not check GitHub right now, but you can still view the latest releases manually.',
        latestExtensionReleaseFallback
      );
    }
  }
}

document.getElementById('copy-pref').addEventListener('click', async () => {
  await copyText(prefKey);
});

document.getElementById('open-config').addEventListener('click', () => {
  openFirefoxInternalPage(
    'about:config',
    'Open about:config manually',
    'Firefox blocked direct navigation to about:config from the extension setup page.'
  );
});

document.getElementById('download-ca').addEventListener('click', () => {
  browser.tabs.create({ url: latticeConfigApi.caCertUrl(latticeConfigApi.defaults()) });
});

document.getElementById('open-certs').addEventListener('click', () => {
  openFirefoxInternalPage(
    'about:preferences#privacy',
    'Open certificate settings manually',
    'Firefox blocked direct navigation to the certificate settings page from the extension setup page.'
  );
});

document.getElementById('open-amo').addEventListener('click', () => {
  browser.tabs.create({ url: amoUrl });
});

document.getElementById('check-release').addEventListener('click', () => {
  checkForLatestRelease(true);
});

document.getElementById('done-test').addEventListener('click', async () => {
  const tabs = await browser.tabs.query({ active: true, currentWindow: true });
  if (tabs.length > 0 && tabs[0].id) {
    browser.tabs.update(tabs[0].id, { url: 'https://benjf.loom' });
  } else {
    browser.tabs.create({ url: 'https://benjf.loom' });
  }
});

checkForLatestRelease(false);
