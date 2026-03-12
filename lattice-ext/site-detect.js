(function() {
  function logStatus(message) {
    console.info('[Lattice site-detect]', message, window.location.href);
  }

  function updateBadge() {
    var badge = document.getElementById("extension-badge");
    var badgeText = document.getElementById("extension-badge-text");
    if (!badge || !badgeText) {
      logStatus('badge elements not found');
      return false;
    }
    badge.dataset.extensionState = "installed";
    badgeText.textContent = "Connected to the Lattice network";
    logStatus('badge updated to installed');
    return true;
  }

  function markInstalled() {
    document.documentElement.setAttribute("data-lattice-extension", "installed");
    if (document.body) {
      document.body.setAttribute("data-lattice-extension", "installed");
    }
    updateBadge();
    document.dispatchEvent(new CustomEvent("lattice-extension-ready"));
    window.postMessage({ type: "lattice-extension-ready" }, window.location.origin);
    logStatus('content script injected');
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", markInstalled, { once: true });
    logStatus('waiting for DOMContentLoaded');
  }
  markInstalled();
})();
