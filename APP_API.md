# Lattice App API (Draft v1)

This document defines a first-pass contract for apps built on Lattice
(including `fray`), with guardrails to avoid network abuse.

## Goals

- Let third-party apps publish/read data over Lattice.
- Keep core protocol stable.
- Prevent one app from degrading small networks.

## Data Model

Apps use namespaced keys:

- `app:{app_id}:{kind}:{id}`

Examples:

- `app:fray:feed:lattice`
- `app:chat:room:general`

Reserved key prefixes remain owned by core protocol:

- `name:`
- `site:`
- `block:`

## Baseline Rules

1. App payloads must be JSON.
2. App IDs must be lowercase `[a-z0-9-]`.
3. Records should include:
   - `version`
   - `generated_at` (unix seconds)
   - `publisher` (public key or app-level identity)
4. Unknown versions must be ignored (forward compatibility).

## Suggested Limits

- Max app record payload: 256 KiB
- Max write rate per app/node: configurable token bucket
- Max retained bytes per app/node: configurable quota
- TTL + eviction for unpinned app data

## Integrity

- App records should include signature fields for tamper resistance.
- Clients must verify schema + signature before accepting data.
- Reject records with timestamps too far in the future.

## Fray v1 Mapping

- Feed key: `app:fray:feed:{fray}`
- Value: JSON object:
  - `version: 1`
  - `fray: string`
  - `generated_at: u64`
  - `posts: Post[]`
  - `comments: Comment[]`

Current implementation guardrails:
- feed max size: 256 KiB
- max posts per feed publish: 20
- max comments per feed publish: 200 (10 per post)
- post body max: 4000 chars
- comment body max: 1200 chars

## RPC Usage (Current)

Apps currently use existing daemon JSON-RPC:

- `put_record(key, value)`
- `get_record(key)`

Future: dedicated app RPCs with builtin quotas and schema checks.
