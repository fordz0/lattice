# Lattice Daemon

Lattice is a censorship-resistant peer-to-peer protocol. The `lattice-daemon` process runs
node identity, peer discovery, transport security, and DHT coordination for local applications.

Run two local instances to test mDNS peer discovery:

```bash
# Terminal 1
cargo run -p lattice-daemon

# Terminal 2 — different port
LATTICE_PORT=7782 LATTICE_RPC_PORT=7783 LATTICE_HTTP_PORT=7784 cargo run -p lattice-daemon

# You should see both nodes discover each other via mDNS and log the connection.
```
