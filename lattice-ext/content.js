(function() {
  var overlayExpanded = null;
  var currentState = null;

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
      return 'Unknown';
    }
    try {
      return new Date(unixSeconds * 1000).toLocaleDateString();
    } catch (_e) {
      return 'Unknown';
    }
  }

  function chipPalette(status, explicitlyTrusted) {
    if (status === 'key_changed') {
      return {
        card: 'linear-gradient(180deg, rgba(43, 25, 6, 0.98) 0%, rgba(26, 18, 8, 0.98) 100%)',
        border: 'rgba(233, 177, 72, 0.45)',
        text: '#f4d38c',
        tone: '#d9921f',
        label: 'Key changed'
      };
    }
    if (explicitlyTrusted) {
      return {
        card: 'linear-gradient(180deg, rgba(9, 31, 20, 0.98) 0%, rgba(8, 23, 16, 0.98) 100%)',
        border: 'rgba(74, 195, 126, 0.4)',
        text: '#b9f2cf',
        tone: '#2cb36b',
        label: 'Trusted'
      };
    }
    if (status === 'matches') {
      return {
        card: 'linear-gradient(180deg, rgba(21, 28, 35, 0.98) 0%, rgba(14, 19, 24, 0.98) 100%)',
        border: 'rgba(115, 131, 149, 0.35)',
        text: '#d8e2ec',
        tone: '#7d91a3',
        label: 'Seen before'
      };
    }
    return {
      card: 'linear-gradient(180deg, rgba(18, 26, 35, 0.98) 0%, rgba(12, 19, 26, 0.98) 100%)',
      border: 'rgba(88, 126, 161, 0.35)',
      text: '#dce8f4',
      tone: '#6f94b8',
      label: 'First visit'
    };
  }

  function trustCopy(state) {
    var trust = state && state.manifest ? state.manifest.trust : null;
    var known = state ? state.known : null;
    var status = trust ? trust.status : 'offline';
    var explicitlyTrusted = !!(known && known.explicitly_trusted);

    if (!state || !trust) {
      return {
        title: 'Daemon unavailable',
        body: 'Lattice could not read trust data for this site.',
        action: null
      };
    }

    if (status === 'key_changed') {
      return {
        title: 'Publisher key changed',
        body: 'This site no longer matches the key first seen on ' + formatDate(trust.first_seen_at) + '.',
        action: explicitlyTrusted ? 'Remove trust' : 'Trust again'
      };
    }

    if (explicitlyTrusted) {
      return {
        title: 'Trusted and cached',
        body: 'This publisher matches your trusted record and the site is pinned locally.',
        action: 'Remove trust'
      };
    }

    if (status === 'matches') {
      return {
        title: 'Known publisher',
        body: 'This matches the publisher key from an earlier visit.',
        action: 'Trust site'
      };
    }

    return {
      title: 'First visit',
      body: 'You have not explicitly trusted this site yet.',
      action: 'Trust site'
    };
  }

  function buildCard(siteName, state) {
    var trust = state && state.manifest ? state.manifest.trust : null;
    var known = state ? state.known : null;
    var status = trust ? trust.status : 'offline';
    var explicitlyTrusted = !!(known && known.explicitly_trusted);
    var palette = chipPalette(status, explicitlyTrusted);
    var copy = trustCopy(state);
    var expanded = overlayExpanded;
    if (expanded === null) {
      expanded = !explicitlyTrusted || status === 'key_changed' || !trust;
    }

    var card = document.createElement('section');
    card.id = 'lattice-trust-card';
    card.style.position = 'fixed';
    card.style.top = '16px';
    card.style.right = '16px';
    card.style.zIndex = '2147483647';
    card.style.width = '248px';
    card.style.padding = '12px';
    card.style.borderRadius = '16px';
    card.style.background = palette.card;
    card.style.border = '1px solid ' + palette.border;
    card.style.boxShadow = '0 18px 40px rgba(0, 0, 0, 0.28)';
    card.style.color = palette.text;
    card.style.fontFamily = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
    card.style.backdropFilter = 'blur(14px)';

    var eyebrowRow = document.createElement('div');
    eyebrowRow.style.display = 'flex';
    eyebrowRow.style.alignItems = 'center';
    eyebrowRow.style.justifyContent = 'space-between';
    eyebrowRow.style.gap = '8px';
    eyebrowRow.style.marginBottom = '6px';

    var eyebrow = document.createElement('div');
    eyebrow.textContent = 'Lattice';
    eyebrow.style.fontSize = '10px';
    eyebrow.style.letterSpacing = '0.14em';
    eyebrow.style.textTransform = 'uppercase';
    eyebrow.style.opacity = '0.72';
    eyebrowRow.appendChild(eyebrow);

    var close = document.createElement('button');
    close.type = 'button';
    close.textContent = '×';
    close.setAttribute('aria-label', 'Hide trust card');
    close.style.width = '22px';
    close.style.height = '22px';
    close.style.border = '0';
    close.style.borderRadius = '999px';
    close.style.padding = '0';
    close.style.cursor = 'pointer';
    close.style.font = 'inherit';
    close.style.fontSize = '16px';
    close.style.lineHeight = '22px';
    close.style.color = palette.text;
    close.style.background = 'rgba(255, 255, 255, 0.12)';
    close.addEventListener('click', function() {
      browser.runtime.sendMessage({
        type: 'hideOverlay',
        siteName: siteName
      }).catch(function() {
        return null;
      });
      var existing = document.getElementById('lattice-trust-card');
      if (existing) {
        existing.remove();
      }
    });
    eyebrowRow.appendChild(close);
    card.appendChild(eyebrowRow);

    var site = document.createElement('div');
    site.textContent = siteName + '.loom';
    site.style.fontSize = '11px';
    site.style.fontWeight = '700';
    site.style.whiteSpace = 'normal';
    site.style.overflow = 'visible';
    site.style.overflowWrap = 'anywhere';
    site.style.wordBreak = 'break-word';
    site.style.lineHeight = '1.35';
    site.style.display = 'block';
    site.style.marginBottom = '8px';
    card.appendChild(site);

    var row = document.createElement('div');
    row.style.display = 'flex';
    row.style.alignItems = 'center';
    row.style.justifyContent = 'space-between';
    row.style.gap = '8px';

    var toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.textContent = expanded ? '▾' : '▸';
    toggle.setAttribute('aria-label', expanded ? 'Collapse trust details' : 'Expand trust details');
    toggle.style.width = '14px';
    toggle.style.height = '14px';
    toggle.style.border = '0';
    toggle.style.padding = '0';
    toggle.style.cursor = 'pointer';
    toggle.style.font = 'inherit';
    toggle.style.fontSize = '13px';
    toggle.style.lineHeight = '14px';
    toggle.style.color = palette.text;
    toggle.style.background = 'transparent';
    toggle.style.flex = '0 0 auto';
    toggle.addEventListener('click', function() {
      overlayExpanded = !expanded;
      render(siteName, state);
    });
    row.appendChild(toggle);

    var summary = document.createElement('div');
    summary.textContent = copy.title;
    summary.style.fontSize = '12px';
    summary.style.fontWeight = '700';
    summary.style.opacity = '0.96';
    summary.style.flex = '1';
    summary.style.whiteSpace = 'nowrap';
    summary.style.minWidth = '0';
    row.appendChild(summary);

    var chip = document.createElement('div');
    chip.textContent = palette.label;
    chip.style.padding = '4px 7px';
    chip.style.borderRadius = '999px';
    chip.style.fontSize = '10px';
    chip.style.fontWeight = '700';
    chip.style.color = '#081017';
    chip.style.background = palette.tone;
    row.appendChild(chip);

    card.appendChild(row);

    if (!expanded && !explicitlyTrusted && copy.action) {
      var compactButton = document.createElement('button');
      compactButton.type = 'button';
      compactButton.textContent = copy.action;
      compactButton.style.marginTop = '10px';
      compactButton.style.width = '100%';
      compactButton.style.border = '0';
      compactButton.style.borderRadius = '12px';
      compactButton.style.padding = '9px 11px';
      compactButton.style.cursor = 'pointer';
      compactButton.style.font = 'inherit';
      compactButton.style.fontSize = '12px';
      compactButton.style.fontWeight = '700';
      compactButton.style.color = '#081017';
      compactButton.style.background = palette.tone;
      compactButton.addEventListener('click', function() {
        compactButton.disabled = true;
        browser.runtime.sendMessage({
          type: 'trustSite',
          siteName: siteName,
          pin: true
        }).catch(function() {
          compactButton.disabled = false;
        });
      });
      card.appendChild(compactButton);
    }

    if (!expanded) {
      return card;
    }

    var title = document.createElement('div');
    title.textContent = copy.body;
    title.style.fontSize = '12px';
    title.style.fontWeight = '400';
    title.style.lineHeight = '1.5';
    title.style.marginTop = '8px';
    title.style.opacity = '0.9';
    card.appendChild(title);

    if (known && known.first_seen_at) {
      var meta = document.createElement('div');
      meta.textContent = 'First seen: ' + formatDate(known.first_seen_at);
      meta.style.marginTop = '10px';
      meta.style.fontSize = '11px';
      meta.style.opacity = '0.72';
      card.appendChild(meta);
    }

    if (copy.action) {
      var button = document.createElement('button');
      button.type = 'button';
      button.textContent = copy.action;
      button.style.marginTop = '12px';
      button.style.width = '100%';
      button.style.border = '0';
      button.style.borderRadius = '12px';
      button.style.padding = '10px 12px';
      button.style.cursor = 'pointer';
      button.style.font = 'inherit';
      button.style.fontSize = '12px';
      button.style.fontWeight = '700';
      button.style.color = '#081017';
      button.style.background = palette.tone;
      button.addEventListener('click', function() {
        button.disabled = true;
        browser.runtime.sendMessage(
          explicitlyTrusted
            ? { type: 'untrustSite', siteName: siteName, unpin: false }
            : { type: 'trustSite', siteName: siteName, pin: true }
        ).catch(function() {
          button.disabled = false;
        });
      });
      card.appendChild(button);
    }

    return card;
  }

  function render(siteName, state) {
    var existing = document.getElementById('lattice-trust-card');
    if (existing) {
      existing.remove();
    }
    currentState = state;
    if (state && state.overlayHidden) {
      return;
    }
    if (!document.body) {
      return;
    }
    document.body.appendChild(buildCard(siteName, state));
  }

  var siteName = siteNameFromHost(window.location.hostname);
  if (!siteName) {
    return;
  }

  function loadState() {
    return browser.runtime.sendMessage({
      type: 'getSiteState',
      siteName: siteName
    }).then(function(state) {
      overlayExpanded = null;
      render(siteName, state);
    }).catch(function() {
      return null;
    });
  }

  browser.runtime.onMessage.addListener(function(message) {
    if (!message || message.type !== 'trustStateChanged' || message.siteName !== siteName) {
      return undefined;
    }
    overlayExpanded = null;
    render(siteName, message.state);
    return undefined;
  });

  function start() {
    loadState();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
