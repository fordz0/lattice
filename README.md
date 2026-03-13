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
- Linux, macOS, or Windows

### Install options

Arch Linux:
```bash
yay -S lattice-net-git
```

macOS and Windows:
- download the latest release artifacts from:
  `https://github.com/fordz0/lattice/releases/latest`
- Windows users should prefer the `.msi`
- macOS users should prefer the matching `.tar.gz` for their CPU

Build from source on any platform:

```bash
git clone https://github.com/fordz0/lattice.git
cd lattice
cargo build --release -p lattice-daemon -p lattice
```

### Run the daemon

Bring the local daemon online:
```bash
lattice up
lattice doctor
```

This starts or enables `lattice-daemon`, connects you to the default
bootstrap node, and waits for the daemon to become ready. One daemon is
enough for normal use; running multiple local nodes is optional and mainly
useful for advanced testing/operator workflows.

If something feels off, run `lattice doctor` for a quick health check and
actionable next steps.

For scripts or CI, `lattice doctor --json --strict` will emit a machine-readable
report and exit nonzero if the local setup is unhealthy.

### Basic commands
```bash
# check your node status
lattice status
lattice doctor

# optionally claim a .loom name up front
# (publish auto-claims if the name is currently unclaimed)
lattice name claim yourname

# create a new site in the current directory
mkdir mysite && cd mysite
lattice init --name yourname

# publish your site to the network
lattice publish

# fetch someone else's site
lattice fetch website --out ./website

# update installed lattice apps from the registry
lattice update --all
```

### Visit .loom sites in Firefox

- Firefox extension install steps: [lattice-ext/INSTALL.md](lattice-ext/INSTALL.md)
- For secure lock icons, import the local Lattice CA (`/__lattice_ca.pem`) as documented.
- The extension keeps `https://*.loom` in the URL bar while routing traffic through your local daemon proxy.
- You can also use `lattice fetch` to retrieve site content locally.

---

## For Node Operators

Want to help keep the network alive by running a bootstrap node?
See [BOOTSTRAP.md](BOOTSTRAP.md).

## For App Builders

Draft app-layer guidance lives in [APP_API.md](APP_API.md).

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

---

## Licence

GPL-3.0 — see [LICENSE](LICENSE).
