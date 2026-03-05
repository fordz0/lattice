# Lattice

A censorship-resistant, peer-to-peer internet protocol. Publish and
access websites without DNS, registrars, or hosting providers. Sites
live at human-readable `.loom` addresses, owned by cryptographic
keypairs, served directly by peers.

Traffic looks like HTTPS to anyone watching. No central authority.
No single point anyone can lean on to take something down.

> ⚠️ Early development — expect rough edges. Protocol may change.

---

## For Users

### Requirements

- Rust installed ([rustup.rs](https://rustup.rs))
- Linux or macOS — Windows untested but may work

### Install
```bash
git clone https://github.com/fordz0/lattice.git
cd lattice
cargo build --release
```

### Run the daemon

Open a terminal and keep it running:
```bash
./target/release/lattice-daemon
```

This connects you to the Lattice network automatically via the
default bootstrap node. Leave it running while you use Lattice.
One daemon is enough for normal use; running multiple local nodes is
optional and mainly useful for advanced testing/operator workflows.

### Basic commands
```bash
# check your node status
./target/release/lattice-cli status

# optionally claim a .loom name up front
# (publish auto-claims if the name is currently unclaimed)
./target/release/lattice-cli name claim yourname

# create a new site in the current directory
mkdir mysite && cd mysite
./target/release/lattice-cli init --name yourname

# publish your site to the network
./target/release/lattice-cli publish

# fetch someone else's site
./target/release/lattice-cli fetch website --out ./website
```

### Visit .loom sites in Firefox

- Firefox extension install steps: [lattice-ext/INSTALL.md](lattice-ext/INSTALL.md)
- For secure lock icons, import the local Lattice CA (`/__lattice_ca.pem`) as documented.
- The extension keeps `https://*.loom` in the URL bar while routing traffic through your local daemon proxy.
- You can also use `lattice-cli fetch` to retrieve site content locally.

---

## For Node Operators

Want to help keep the network alive by running a bootstrap node?
See [BOOTSTRAP.md](BOOTSTRAP.md).

---

## How it works

- **Names** are first-come-first-served DHT records signed by your
  Ed25519 keypair. No registrar. Names expire after 30 days of
  inactivity so squatters lose them automatically.
- **Content** is chunked, SHA-256 hashed, and served directly from
  the publisher's machine. Peers cache blocks they've fetched,
  providing organic resilience.
- **Traffic** uses encrypted libp2p transports (Noise over TCP and QUIC).
- **No hidden services** — Lattice is a privacy layer for the normal
  open web, not a dark web.

---

## Current bootstrap nodes

- `kraken` — Frankfurt, DE — operated by [@fordz0](https://github.com/fordz0)

---

## Status

- [x] P2P daemon with Kademlia DHT
- [x] mDNS local peer discovery
- [x] Internet connectivity via bootstrap peers
- [x] CLI — status, peers, put, get, name claim/info, init, publish, fetch
- [x] Site publishing with Ed25519 manifest signing
- [x] Firefox extension
- [x] Relay-assisted NAT reachability
- [x] Name heartbeat and expiry enforcement
- [ ] Loom chat protocol

---

## Licence

GPL-3.0 — see [LICENSE](LICENSE).
