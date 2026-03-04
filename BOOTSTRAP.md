# Running a Lattice Bootstrap Node

Thanks for helping keep the Lattice network alive.

## Requirements
- A VPS with a static public IP
- Port 7779 open TCP and UDP
- Git and Rust installed

## Install Rust
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

## Install build tools (Ubuntu/Debian)
```bash
sudo apt install build-essential
```

## Clone and build
```bash
git clone https://github.com/fordz0/lattice.git
cd lattice
cargo build --release -p lattice-daemon
cargo build --release -p lattice-cli
```

## Run as a systemd service

Create the service file:
```bash
sudo nano /etc/systemd/system/lattice-daemon.service
```

Paste this, replacing `YOUR_USER` with your username:
```ini
[Unit]
Description=Lattice P2P Daemon
After=network.target

[Service]
ExecStart=/home/YOUR_USER/lattice/target/release/lattice-daemon
Restart=always
RestartSec=5
User=YOUR_USER
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

Then enable and start it:
```bash
sudo systemctl daemon-reload
sudo systemctl enable lattice-daemon
sudo systemctl start lattice-daemon
sudo systemctl status lattice-daemon
```

## Send us your node details

Once running, grab your peer ID:
```bash
~/lattice/target/release/lattice-cli --rpc-port 7780 status
```

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

## Current bootstrap nodes
- `/ip4/188.245.245.179/tcp/7779/p2p/12D3KooWQQw51zoUZuVKoraBuAqkts7gX8qe2yQ1ZgTAoFVfCQFD` — (Frankfurt)
