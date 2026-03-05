# Lattice Daemon

Lattice is a censorship-resistant peer-to-peer protocol. The `lattice-daemon` process runs
node identity, peer discovery, transport security, and DHT coordination for local applications.

Run two local instances to test mDNS peer discovery:

```bash
# Terminal 1 (isolated data dir)
LATTICE_DATA_DIR=/tmp/lattice-node1 \
  cargo run -p lattice-daemon

# Terminal 2 — isolated data dir + different ports
LATTICE_DATA_DIR=/tmp/lattice-node2 \
LATTICE_PORT=8782 \
LATTICE_RPC_PORT=8783 \
LATTICE_HTTP_PORT=8784 \
LATTICE_HTTPS_PORT=8743 \
LATTICE_PROXY_PORT=8785 \
  cargo run -p lattice-daemon

# You should see both nodes discover each other via mDNS and log the connection.
```
