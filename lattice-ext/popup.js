(function() {
  function isLoomHost(hostname) {
    return typeof hostname === 'string' && hostname.slice(-5) === '.loom';
  }

  function siteNameFromUrl(urlString) {
    try {
      var url = new URL(urlString);
      if (!isLoomHost(url.hostname)) {
        return null;
      }
      return url.hostname.slice(0, -5);
    } catch (_e) {
      return null;
    }
  }

  function truncateKey(value) {
    if (!value) {
      return 'Unknown';
    }
    if (value.length <= 18) {
      return value;
    }
    return value.slice(0, 12) + '…' + value.slice(-6);
  }

  function formatDate(unixSeconds) {
    if (!unixSeconds) {
      return 'Unknown';
    }
    return new Date(unixSeconds * 1000).toLocaleString();
  }

  function setError(message) {
    var error = document.getElementById('error');
    error.textContent = message;
    error.style.display = message ? 'block' : 'none';
  }

  function renderNoSite() {
    document.getElementById('content').innerHTML =
      '<p id="status">No Lattice site active</p>';
    setError('');
  }

  function renderSite(siteName, state) {
    var known = state.known;
    var trust = state.manifest ? state.manifest.trust : null;
    var explicitlyTrusted = !!(known && known.explicitly_trusted);
    var buttonLabel = explicitlyTrusted ? 'Remove trust' : 'Trust and cache this site';
    var buttonClass = explicitlyTrusted ? 'secondary' : '';
    var statusText = trust && trust.status === 'key_changed'
      ? 'Publisher key changed since first visit'
      : explicitlyTrusted
        ? 'Trusted'
        : 'Observed';

    var html = ''
      + '<p id="status">' + statusText + '</p>'
      + '<dl>'
      + '<dt>Site</dt><dd>' + siteName + '.loom</dd>'
      + '<dt>Publisher Key</dt><dd>' + truncateKey(known && known.publisher_b64) + '</dd>'
      + '<dt>First Seen</dt><dd>' + formatDate(known && known.first_seen_at) + '</dd>'
      + '<dt>Pinned Blocks</dt><dd>' + (state.pinned ? 'Yes' : 'No') + '</dd>'
      + '</dl>'
      + '<button id="trustButton" class="' + buttonClass + '">' + buttonLabel + '</button>';

    document.getElementById('content').innerHTML = html;
    setError('');

    document.getElementById('trustButton').addEventListener('click', function() {
      var button = this;
      button.disabled = true;
      var message = explicitlyTrusted
        ? { type: 'untrustSite', siteName: siteName, unpin: false }
        : { type: 'trustSite', siteName: siteName, pin: true };

      browser.runtime.sendMessage(message).then(function(response) {
        if (!response || response.status !== 'ok') {
          throw new Error(response && response.error ? response.error : 'operation failed');
        }
        loadPopup();
      }).catch(function(error) {
        button.disabled = false;
        setError(error && error.message ? error.message : 'operation failed');
      });
    });
  }

  function loadPopup() {
    browser.tabs.query({ active: true, currentWindow: true }).then(function(tabs) {
      var tab = tabs && tabs[0];
      var siteName = tab && siteNameFromUrl(tab.url);
      if (!siteName) {
        renderNoSite();
        return;
      }

      browser.runtime.sendMessage({
        type: 'getPopupState',
        siteName: siteName
      }).then(function(state) {
        if (!state || !state.known) {
          renderNoSite();
          return;
        }
        renderSite(siteName, state);
      }).catch(function(error) {
        renderNoSite();
        setError(error && error.message ? error.message : 'failed to query daemon');
      });
    }).catch(function(error) {
      renderNoSite();
      setError(error && error.message ? error.message : 'failed to inspect active tab');
    });
  }

  document.addEventListener('DOMContentLoaded', loadPopup);
})();
