# Fray

`fray` is an early Reddit-like service model for Lattice (`f.loom`).

Current scope:
- Route model (`/f/{fray}`, `/f/{fray}/{post}`, `/u/{user}`)
- Persistent local post storage (sled)
- Minimal local API

## Run

```bash
cargo run -p fray
```

Config:
- `FRAY_PORT` (default: `8890`)
- `FRAY_DATA_DIR` (default: `~/.lattice/fray`)

## API

- `GET /health`
- `GET /api/f/{fray}/posts?limit=50`
- `POST /api/f/{fray}/posts`
- `GET /api/f/{fray}/posts/{post_id}`

Example:

```bash
curl -sS -X POST http://127.0.0.1:8890/api/f/lattice/posts \
  -H 'content-type: application/json' \
  -d '{"author":"fordz0","title":"First post","body":"hello from fray"}'
```
