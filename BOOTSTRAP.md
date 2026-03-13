# Running a Lattice Bootstrap Node

Thanks for helping keep the Lattice network alive.

## Requirements
- A VPS with a static public IP
- Port 7779 open TCP and UDP
- A long-lived user account that will own the daemon state

## Install

### Debian and Ubuntu

Install from the published APT repo or a release `.deb`:

```bash
sudo apt install lattice
```

This gives you:

- `/usr/bin/lattice`
- `/usr/bin/lattice-daemon`

The package also installs a **user** systemd unit, but for a real bootstrap VPS
you will usually want a **system** service so the node starts on boot without a
login session.

### Source build

If you prefer building from source:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
git clone https://github.com/fordz0/lattice.git
cd lattice
cargo build --release -p lattice-daemon -p lattice
```

## Bootstrap setup shortcut

On a Linux VPS, the fastest path is now:

```bash
sudo lattice up --bootstrap
```

That installs or refreshes a system service, disables conflicting user-level
`lattice-daemon` units where possible, and starts the daemon in a
server-oriented configuration.

## Run as a systemd service manually

Create the service file:
```bash
sudo nano /etc/systemd/system/lattice-daemon.service
```

Paste this, replacing `YOUR_USER` with the user that should own the daemon
identity and state:
```ini
[Unit]
Description=Lattice P2P Daemon
After=network.target

[Service]
ExecStart=/usr/bin/lattice-daemon
Restart=always
RestartSec=5
User=YOUR_USER
Environment=RUST_LOG=info
Environment=LATTICE_DATA_DIR=/home/YOUR_USER/.lattice

[Install]
WantedBy=multi-user.target
```

If you are running from a source checkout instead of the Debian package, change
`ExecStart` back to the built binary path, for example:

```ini
ExecStart=/home/YOUR_USER/lattice/target/release/lattice-daemon
```

Then enable and start it:
```bash
sudo systemctl daemon-reload
sudo systemctl enable lattice-daemon
sudo systemctl start lattice-daemon
sudo systemctl status lattice-daemon
```

If the machine is a normal always-on site host rather than a bootstrap node,
use this shortcut instead:

```bash
sudo lattice up --server
```

That gives you the same system-service behavior without the bootstrap-specific
guidance.

When you upgrade the Linux package later, active `lattice-daemon` services are
restarted automatically so the node picks up the new binaries without manual
service restarts.

## Safe migration from a source-built node to the APT package

Yes, you can safely move an existing source-built server to the APT package.
The important part is to keep using the same data directory and service user so
the node keeps its identity key, site signing key, config, and local records.

For a source-built node currently running as `YOUR_USER`:

1. Stop the old service:
   ```bash
   sudo systemctl stop lattice-daemon
   ```
2. Install the package:
   ```bash
   sudo apt install lattice
   ```
3. Either:
   - run `sudo lattice up --server`, or
   - point your systemd service at `/usr/bin/lattice-daemon`
4. Keep:
   ```ini
   User=YOUR_USER
   Environment=LATTICE_DATA_DIR=/home/YOUR_USER/.lattice
   ```
5. Reload and start again:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl start lattice-daemon
   ```

That preserves the node identity. If you change the user or data directory, the
daemon will generate a new identity and the network will see it as a different
node.

## Send us your node details

Once running, grab your peer ID:
```bash
/usr/bin/lattice --rpc-port 7780 status
```

If you are still running from source, use your built CLI path instead.

Send your public IP and peer ID to be added to the default bootstrap
list so new nodes find you automatically. Format looks like:
```
/ip4/YOUR_PUBLIC_IP/tcp/7779/p2p/YOUR_PEER_ID
```

## Verify it's working
```bash
curl -s -X POST http://localhost:7780 \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"node_info","params":[],"id":1}'
```

You should see your peer ID and listen addresses in the response.

## Notes for site-hosting nodes

If your server is currently hosting `lattice.loom`, `benjf.loom`, or
`fray.loom` from a source-built daemon, the same migration rule applies:

- keep the same service user
- keep the same `LATTICE_DATA_DIR`
- switch only the executable path to `/usr/bin/lattice-daemon`

That is the safe way to move those services onto the packaged binaries without
rotating the node identity underneath them.

## Current bootstrap nodes
- `/ip4/188.245.245.179/tcp/7779/p2p/12D3KooWQQw51zoUZuVKoraBuAqkts7gX8qe2yQ1ZgTAoFVfCQFD` — (Frankfurt)
