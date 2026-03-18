const prefKey = document.getElementById('pref-key').textContent.trim();
const amoUrl = 'https://addons.mozilla.org/en-US/firefox/addon/lattice/';
const latestExtensionReleaseApi = 'https://api.github.com/repos/fordz0/lattice/releases?per_page=20';
const latestExtensionReleaseFallback = 'https://github.com/fordz0/lattice/releases?q=lattice-ext-v&expanded=true';
const latestExtensionReleaseDownloads = 'https://github.com/fordz0/lattice/releases/latest';
const browserApi = typeof browser !== 'undefined'
  ? browser
  : (typeof chrome !== 'undefined' ? chrome : null);
const installPanelTitle = document.getElementById('install-panel-title');
const installPanelCopy = document.getElementById('install-panel-copy');
const releasePanelTitle = document.getElementById('release-panel-title');
const releasePanelCopy = document.getElementById('release-panel-copy');
const setupCopy = document.getElementById('setup-copy');
const firefoxSetup = document.getElementById('firefox-setup');
const chromiumSetup = document.getElementById('chromium-setup');
const copyPrefButton = document.getElementById('copy-pref');
const openInstallPageButton = document.getElementById('open-install-page');

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

function detectBrowserFamily() {
  const source = String(navigator.userAgent || '').toLowerCase();
  if (source.indexOf('firefox') !== -1) {
    return 'firefox';
  }
  if (
    source.indexOf('chrome') !== -1 ||
    source.indexOf('chromium') !== -1 ||
    source.indexOf('edg/') !== -1 ||
    source.indexOf('opr/') !== -1
  ) {
    return 'chromium';
  }
  return 'unknown';
}

const browserFamily = detectBrowserFamily();

function setReleaseNotice(copy, htmlUrl) {
  document.getElementById('release-copy').textContent = copy;
  const notice = document.getElementById('release-notice');
  notice.hidden = false;
  const button = document.getElementById('view-release');
  button.onclick = function() {
    browserApi.tabs.create({ url: htmlUrl });
  };
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
    const manifestVersion = browserApi.runtime.getManifest().version;
    const releaseVersion = latticeSetupHelpers.parseGithubReleaseVersion(release.tag_name);
    if (latticeSetupHelpers.compareVersions(releaseVersion, manifestVersion) > 0) {
      setReleaseNotice(
        (browserFamily === 'chromium' ? 'Chromium preview build ' : 'Version ') +
          releaseVersion +
          ' is available on GitHub. You are running extension version ' +
          manifestVersion +
          '.',
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

if (browserFamily === 'chromium') {
  installPanelTitle.textContent = 'Chromium preview';
  installPanelCopy.textContent = 'Use the unpacked Chromium build from GitHub Releases. Direct website installs of unsigned .crx files are usually blocked.';
  releasePanelTitle.textContent = 'Latest Chromium preview';
  releasePanelCopy.textContent = 'We can check the latest packaged Chromium release on GitHub and point you at it if this preview build is behind.';
  setupCopy.innerHTML = "Chromium needs the extension to control the local proxy and still needs Lattice's local HTTPS certificate authority trusted before <code>https://name.loom</code> will load cleanly.";
  firefoxSetup.hidden = true;
  chromiumSetup.hidden = false;
  copyPrefButton.hidden = true;
} else {
  copyPrefButton.addEventListener('click', async () => {
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
}

document.getElementById('download-ca').addEventListener('click', () => {
  browserApi.tabs.create({ url: latticeConfigApi.caCertUrl(latticeConfigApi.defaults()) });
});

openInstallPageButton.addEventListener('click', () => {
  browserApi.tabs.create({
    url: browserFamily === 'chromium' ? latestExtensionReleaseDownloads : amoUrl
  });
});

document.getElementById('check-release').addEventListener('click', () => {
  checkForLatestRelease(true);
});

document.getElementById('done-test').addEventListener('click', async () => {
  const tabs = await browserApi.tabs.query({ active: true, currentWindow: true });
  if (tabs.length > 0 && tabs[0].id) {
    browserApi.tabs.update(tabs[0].id, { url: 'https://benjf.loom' });
  } else {
    browserApi.tabs.create({ url: 'https://benjf.loom' });
  }
});

checkForLatestRelease(false);
