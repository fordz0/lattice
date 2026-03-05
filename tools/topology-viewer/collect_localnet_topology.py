#!/usr/bin/env python3
import argparse
import json
import time
import urllib.error
import urllib.request


def rpc_node_info(rpc_port: int, timeout_secs: float):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "node_info",
        "params": [],
    }
    request = urllib.request.Request(
        f"http://127.0.0.1:{rpc_port}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_secs) as resp:
        raw = resp.read()
    envelope = json.loads(raw.decode("utf-8"))
    if "error" in envelope:
        raise RuntimeError(str(envelope["error"]))
    return envelope["result"]


def make_label(peer_id: str, idx: int) -> str:
    if not peer_id:
        return f"node{idx}"
    if len(peer_id) < 12:
        return peer_id
    return f"{peer_id[:6]}...{peer_id[-4:]}"


def main():
    parser = argparse.ArgumentParser(description="Collect Lattice localnet topology JSON")
    parser.add_argument("--nodes", type=int, default=4, help="node count (default: 4)")
    parser.add_argument(
        "--base-port", type=int, default=19000, help="localnet base port (default: 19000)"
    )
    parser.add_argument(
        "--timeout-secs", type=float, default=2.5, help="RPC timeout per node (default: 2.5)"
    )
    parser.add_argument(
        "--out", type=str, default="topology.json", help="output JSON path (default: topology.json)"
    )
    args = parser.parse_args()

    if args.nodes < 1:
        raise SystemExit("--nodes must be >= 1")

    nodes = []
    peer_to_node = {}
    for idx in range(1, args.nodes + 1):
        rpc_port = args.base_port + (idx - 1) * 10 + 1
        node = {
            "index": idx,
            "rpc_port": rpc_port,
            "peer_id": None,
            "label": f"node{idx}",
            "connected_peers": 0,
            "connected_peer_ids": [],
            "listen_addrs": [],
            "error": None,
        }
        try:
            info = rpc_node_info(rpc_port, args.timeout_secs)
            node["peer_id"] = info.get("peer_id")
            node["label"] = make_label(info.get("peer_id", ""), idx)
            node["connected_peers"] = info.get("connected_peers", 0)
            node["connected_peer_ids"] = info.get("connected_peer_ids", []) or []
            node["listen_addrs"] = info.get("listen_addrs", []) or []
            if node["peer_id"]:
                peer_to_node[node["peer_id"]] = idx
        except (urllib.error.URLError, TimeoutError, RuntimeError, ValueError) as err:
            node["error"] = str(err)
        nodes.append(node)

    edges = set()
    for node in nodes:
        src_idx = node["index"]
        for peer_id in node["connected_peer_ids"]:
            dst_idx = peer_to_node.get(peer_id)
            if not dst_idx:
                continue
            a, b = sorted((src_idx, dst_idx))
            if a != b:
                edges.add((a, b))

    out = {
        "generated_at_unix": int(time.time()),
        "nodes": nodes,
        "edges": [{"source": a, "target": b} for (a, b) in sorted(edges)],
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    ok_nodes = len([n for n in nodes if not n["error"]])
    print(f"wrote {args.out}: nodes={len(nodes)} ok={ok_nodes} edges={len(edges)}")


if __name__ == "__main__":
    main()
