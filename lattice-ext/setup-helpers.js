(function(global) {
  function parseGithubReleaseVersion(tagName) {
    return String(tagName || '')
      .replace(/^lattice-v/i, '')
      .replace(/^v/i, '')
      .trim();
  }

  function compareVersions(left, right) {
    function parseVersion(value) {
      return String(value || '')
        .split(/[^0-9]+/)
        .filter(Boolean)
        .map(function(part) {
          return parseInt(part, 10) || 0;
        });
    }

    var a = parseVersion(left);
    var b = parseVersion(right);
    var len = Math.max(a.length, b.length);

    for (var index = 0; index < len; index += 1) {
      var av = a[index] || 0;
      var bv = b[index] || 0;
      if (av > bv) return 1;
      if (av < bv) return -1;
    }

    return 0;
  }

  var api = {
    parseGithubReleaseVersion: parseGithubReleaseVersion,
    compareVersions: compareVersions
  };

  global.LatticeSetupHelpers = api;

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
})(typeof globalThis !== 'undefined' ? globalThis : this);
