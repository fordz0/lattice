# Topology Viewer

Simple localnet graph viewer for Lattice nodes.

## 1) Collect topology JSON

```bash
python3 tools/topology-viewer/collect_localnet_topology.py \
  --nodes 4 \
  --base-port 19000 \
  --out tools/topology-viewer/topology.json
```

Defaults:
- `--nodes 4`
- `--base-port 19000`
- `--out topology.json`

## 2) Open the viewer

```bash
cd tools/topology-viewer
python3 -m http.server 8040
```

Then open:
- `http://127.0.0.1:8040/index.html`

Use **Load topology.json** or upload a JSON file with the file picker.
