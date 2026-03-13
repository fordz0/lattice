document.addEventListener("DOMContentLoaded", () => {
  const badge = document.querySelector("#extension-badge");
  const badgeLink = document.querySelector("#extension-badge-link");
  const badgeText = document.querySelector("#extension-badge-text");

  if (!badge || !badgeLink || !badgeText) {
    return;
  }

  const hasExtensionMarker = () =>
    document.documentElement.dataset.latticeExtension === "installed" ||
    document.body?.dataset.latticeExtension === "installed";

  const setBadge = (state) => {
    badge.dataset.extensionState = state;
    badgeLink.href = "/getting-started#extension";
    badgeText.textContent = state === "installed"
      ? "Connected to the Lattice network"
      : "Install the Firefox extension to browse .loom sites";
  };

  const markInstalled = () => setBadge("installed");

  setBadge(hasExtensionMarker() ? "installed" : "missing");
  document.addEventListener("lattice-extension-ready", () => setBadge("installed"));
  window.addEventListener("message", (event) => {
    if (event.source !== window || event.data?.type !== "lattice-extension-ready") {
      return;
    }
    markInstalled();
  });

  const observer = new MutationObserver(() => {
    if (hasExtensionMarker()) {
      markInstalled();
      observer.disconnect();
    }
  });

  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-lattice-extension"],
  });

  if (document.body) {
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ["data-lattice-extension"],
    });
  }

  window.setTimeout(() => {
    if (hasExtensionMarker()) {
      markInstalled();
    }
    observer.disconnect();
  }, 2000);
});
