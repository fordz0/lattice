(function(global) {
  var DEFAULTS = Object.freeze({
    localHost: '127.0.0.1',
    rpcPort: 7780,
    httpPort: 7781,
    proxyPort: 7782
  });

  function defaults() {
    return {
      localHost: DEFAULTS.localHost,
      rpcPort: DEFAULTS.rpcPort,
      httpPort: DEFAULTS.httpPort,
      proxyPort: DEFAULTS.proxyPort
    };
  }

  function daemonRpcUrl(config) {
    return 'http://' + config.localHost + ':' + config.rpcPort;
  }

  function caCertUrl(config) {
    return 'http://' + config.localHost + ':' + config.httpPort + '/__lattice_ca.pem';
  }

  global.LatticeConfig = {
    DEFAULTS: DEFAULTS,
    defaults: defaults,
    daemonRpcUrl: daemonRpcUrl,
    caCertUrl: caCertUrl
  };
})(typeof self !== 'undefined' ? self : window);
