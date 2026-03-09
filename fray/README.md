# Fray

`fray` is an early Reddit-like service model for Lattice (`f.loom`).

Current scope:
- Route model (`/f/{fray}`, `/f/{fray}/{post}`, `/u/{user}`)
- Persistent local thread/comment storage (sled)
- Minimal local API + browser UI
- Network sync through `app:fray:feed:{fray}` records

## Run

```bash
cargo run -p fray
```

Config:
- `FRAY_PORT` (default: `8890`)
- `FRAY_DATA_DIR` (default: `~/.lattice/fray`)
- `FRAY_LATTICE_RPC_PORT` (default: `7780`)

## API

- `GET /`
- `GET /health`
- `GET /api/v1/info`
- `GET /api/f/{fray}/posts?limit=50`
- `POST /api/f/{fray}/posts`
- `GET /api/f/{fray}/posts/{post_id}`
- `GET /api/f/{fray}/posts/{post_id}/comments?limit=200`
- `POST /api/f/{fray}/posts/{post_id}/comments`
- `GET /api/v1/frays/{fray}/posts?limit=50`
- `POST /api/v1/frays/{fray}/posts`
- `GET /api/v1/frays/{fray}/posts/{post_id}`
- `GET /api/v1/frays/{fray}/posts/{post_id}/comments?limit=200`
- `POST /api/v1/frays/{fray}/posts/{post_id}/comments`
- `POST /api/v1/frays/{fray}/sync/publish`
- `POST /api/v1/frays/{fray}/sync/pull`

Example:

```bash
curl -sS -X POST http://127.0.0.1:8890/api/f/lattice/posts \
  -H 'content-type: application/json' \
  -d '{"author":"fordz0","title":"First post","body":"hello from fray"}'
```

Publish/pull via Lattice record key `app:fray:feed:{fray}`:

```bash
curl -sS -X POST http://127.0.0.1:8890/api/v1/frays/lattice/sync/publish
curl -sS -X POST http://127.0.0.1:8890/api/v1/frays/lattice/sync/pull
```

Open UI:

```bash
xdg-open http://127.0.0.1:8890/
```
