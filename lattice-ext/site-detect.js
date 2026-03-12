(function() {
  function markInstalled() {
    document.documentElement.setAttribute("data-lattice-extension", "installed");
    if (document.body) {
      document.body.setAttribute("data-lattice-extension", "installed");
    }
    document.dispatchEvent(new CustomEvent("lattice-extension-ready"));
    window.postMessage({ type: "lattice-extension-ready" }, window.location.origin);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", markInstalled, { once: true });
  }
  markInstalled();
})();
