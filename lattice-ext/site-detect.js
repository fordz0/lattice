(function() {
  document.documentElement.setAttribute("data-lattice-extension", "installed");
  document.dispatchEvent(new CustomEvent("lattice-extension-ready"));
})();
