(function() {
  function isLoomHost(hostname) {
    return typeof hostname === 'string' && hostname.slice(-5) === '.loom';
  }

  function siteNameFromHost(hostname) {
    if (!isLoomHost(hostname)) {
      return null;
    }
    return hostname.slice(0, -5);
  }

  function formatDate(unixSeconds) {
    if (!unixSeconds) {
      return 'an earlier visit';
    }
    try {
      return new Date(unixSeconds * 1000).toLocaleDateString();
    } catch (_e) {
      return 'an earlier visit';
    }
  }

  function buildIndicator(trust) {
    if (!trust || trust.status === 'first_seen') {
      return null;
    }

    var badge = document.createElement('div');
    badge.id = 'lattice-trust-indicator';
    badge.style.position = 'fixed';
    badge.style.top = '12px';
    badge.style.right = '12px';
    badge.style.zIndex = '2147483647';
    badge.style.width = '28px';
    badge.style.height = '28px';
    badge.style.borderRadius = '999px';
    badge.style.display = 'flex';
    badge.style.alignItems = 'center';
    badge.style.justifyContent = 'center';
    badge.style.fontFamily = 'system-ui, sans-serif';
    badge.style.fontSize = '16px';
    badge.style.fontWeight = '700';
    badge.style.boxShadow = '0 4px 12px rgba(0, 0, 0, 0.18)';
    badge.style.border = '1px solid rgba(0, 0, 0, 0.12)';
    badge.style.backdropFilter = 'blur(8px)';

    if (trust.status === 'key_changed') {
      badge.textContent = '!';
      badge.style.color = '#6a3f00';
      badge.style.background = 'rgba(255, 196, 87, 0.95)';
      badge.title = 'Publisher key has changed since your first visit on ' +
        formatDate(trust.first_seen_at);
      return badge;
    }

    badge.textContent = '✓';
    if (trust.explicitly_trusted) {
      badge.style.color = '#0c5b2a';
      badge.style.background = 'rgba(123, 226, 157, 0.95)';
      badge.title = 'Trusted and cached';
    } else {
      badge.style.color = '#44505c';
      badge.style.background = 'rgba(220, 226, 232, 0.95)';
      badge.title = 'Publisher matches previous visit';
    }
    return badge;
  }

  var siteName = siteNameFromHost(window.location.hostname);
  if (!siteName) {
    return;
  }

  browser.runtime.sendMessage({
    type: 'getTrustIndicator',
    siteName: siteName
  }).then(function(trust) {
    var existing = document.getElementById('lattice-trust-indicator');
    if (existing) {
      existing.remove();
    }
    var badge = buildIndicator(trust);
    if (!badge || !document.body) {
      return;
    }
    document.body.appendChild(badge);
  }).catch(function() {
    // Ignore daemon or messaging failures on page load.
  });
})();
