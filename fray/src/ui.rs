pub fn page_html() -> &'static str {
    r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fray</title>
  <style>
    :root {
      --bg: #f6f1e9;
      --ink: #1e2a33;
      --muted: #5f6d75;
      --line: #d8c7b2;
      --panel: #fffaf3;
      --brand: #cc5a21;
      --brand-soft: #f4d8c7;
      --good: #1f8a4d;
      --bad: #a22929;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Gill Sans", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(circle at 12% 0%, #f7ddc6 0, transparent 35%),
        radial-gradient(circle at 88% 0%, #efe2cb 0, transparent 28%),
        var(--bg);
    }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 20px; }
    header { display: flex; justify-content: space-between; gap: 16px; align-items: end; flex-wrap: wrap; }
    h1 {
      margin: 0;
      font-size: clamp(34px, 7vw, 64px);
      line-height: 0.95;
      letter-spacing: 0.02em;
      font-family: "Palatino Linotype", "Book Antiqua", Palatino, serif;
    }
    .sub { margin-top: 8px; color: var(--muted); font-size: 14px; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    input, textarea, button {
      font: inherit;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      background: var(--panel);
      color: var(--ink);
    }
    input, textarea { width: 100%; }
    textarea { min-height: 110px; resize: vertical; }
    button {
      background: linear-gradient(180deg, #ffd6bb 0%, var(--brand-soft) 100%);
      border-color: #d09a74;
      cursor: pointer;
      font-weight: 600;
    }
    button:hover { filter: brightness(0.98); }
    .btn-ghost {
      background: #fff;
      border-color: var(--line);
      font-weight: 500;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.25fr 1fr;
      gap: 14px;
      margin-top: 14px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
    }
    .title {
      margin: 0 0 8px;
      font-size: 17px;
      font-weight: 700;
      letter-spacing: 0.01em;
    }
    .muted { color: var(--muted); font-size: 13px; }
    .row { display: flex; gap: 8px; }
    .posts { display: grid; gap: 10px; }
    .post { border: 1px solid var(--line); border-radius: 10px; padding: 10px; background: #fffdf9; }
    .post:hover { border-color: #cf8f63; }
    .post-title {
      margin: 0;
      font-size: 16px;
      font-weight: 700;
      color: #26333c;
      cursor: pointer;
    }
    .comments { display: grid; gap: 8px; margin-top: 10px; }
    .comment {
      border-left: 3px solid #e6c5a9;
      padding-left: 10px;
      background: #fffdfa;
      border-radius: 4px;
    }
    .status {
      margin-top: 8px;
      min-height: 18px;
      font-size: 13px;
      color: var(--muted);
    }
    .status.ok { color: var(--good); }
    .status.err { color: var(--bad); }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Fray</h1>
        <div class="sub">Distributed threads for Lattice</div>
      </div>
      <div class="toolbar">
        <button id="sync-pull" class="btn-ghost">Pull Network Feed</button>
        <button id="sync-publish">Publish Feed</button>
      </div>
    </header>

    <div class="grid">
      <section class="card">
        <h2 class="title">Fray Feed</h2>
        <div class="row">
          <input id="fray-name" placeholder="fray name (e.g. lattice)" value="lattice">
          <button id="load">Load</button>
        </div>
        <div id="posts" class="posts" style="margin-top:10px;"></div>
      </section>

      <section class="card">
        <h2 class="title">Create Thread</h2>
        <input id="author" placeholder="author (e.g. fordz0)" value="fordz0">
        <input id="title" placeholder="thread title" style="margin-top:8px;">
        <textarea id="body" placeholder="write your thread..."></textarea>
        <button id="create">Post Thread</button>
      </section>
    </div>

    <section class="card" style="margin-top:14px;">
      <h2 class="title">Thread</h2>
      <div id="thread-empty" class="muted">Select a thread to view comments.</div>
      <article id="thread" style="display:none;">
        <h3 id="thread-title" style="margin:0 0 6px;font-size:22px;"></h3>
        <div id="thread-meta" class="muted"></div>
        <p id="thread-body" style="white-space:pre-wrap;"></p>
        <h4 style="margin:14px 0 8px;">Comments</h4>
        <div id="comments" class="comments"></div>
        <div style="margin-top:10px;">
          <input id="comment-author" placeholder="comment author" value="fordz0">
          <textarea id="comment-body" placeholder="add a comment..."></textarea>
          <button id="comment-create">Add Comment</button>
        </div>
      </article>
    </section>

    <div id="status" class="status"></div>
  </div>
  <script>
    const el = (id) => document.getElementById(id);
    const state = { fray: "lattice", postId: null, posts: [] };

    function setStatus(message, kind) {
      const node = el("status");
      node.textContent = message || "";
      node.className = "status" + (kind ? " " + kind : "");
    }

    function esc(text) {
      return String(text || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    function routeTo(fray, postId) {
      if (postId) {
        history.replaceState(null, "", `/f/${fray}/${postId}`);
      } else {
        history.replaceState(null, "", `/f/${fray}`);
      }
    }

    function readRoute() {
      const parts = location.pathname.split("/").filter(Boolean);
      if (parts[0] === "f" && parts[1]) {
        state.fray = parts[1].toLowerCase();
        state.postId = parts[2] || null;
      }
      el("fray-name").value = state.fray;
    }

    async function api(path, init) {
      const res = await fetch(path, init);
      let body = {};
      try { body = await res.json(); } catch (_) {}
      if (!res.ok) {
        const err = body && body.error ? body.error : `HTTP ${res.status}`;
        throw new Error(err);
      }
      return body;
    }

    async function loadPosts() {
      state.fray = el("fray-name").value.trim().toLowerCase();
      if (!state.fray) return;
      setStatus("Loading feed...");
      try {
        const data = await api(`/api/v1/frays/${state.fray}/posts?limit=50`);
        state.posts = data.posts || [];
        renderPosts();
        setStatus(`Loaded ${state.posts.length} threads`, "ok");
        routeTo(state.fray, state.postId);
        if (state.postId) await loadThread(state.postId);
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    function renderPosts() {
      const posts = el("posts");
      if (!state.posts.length) {
        posts.innerHTML = `<div class="muted">No threads yet in /f/${esc(state.fray)}</div>`;
        return;
      }
      posts.innerHTML = state.posts.map((p) => `
        <article class="post">
          <h3 class="post-title" data-id="${esc(p.id)}">${esc(p.title)}</h3>
          <div class="muted">by ${esc(p.author)} · id ${esc(p.id.slice(0, 12))}</div>
        </article>
      `).join("");
      posts.querySelectorAll(".post-title").forEach((node) => {
        node.addEventListener("click", () => loadThread(node.dataset.id));
      });
    }

    async function loadThread(postId) {
      state.postId = postId;
      routeTo(state.fray, postId);
      try {
        const post = await api(`/api/v1/frays/${state.fray}/posts/${postId}`);
        const comments = await api(`/api/v1/frays/${state.fray}/posts/${postId}/comments?limit=200`);
        el("thread-empty").style.display = "none";
        el("thread").style.display = "block";
        el("thread-title").textContent = post.title;
        el("thread-meta").textContent = `by ${post.author} · ${new Date(post.created_at * 1000).toLocaleString()}`;
        el("thread-body").textContent = post.body;
        renderComments(comments.comments || []);
        setStatus("Thread loaded", "ok");
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    function renderComments(comments) {
      const box = el("comments");
      if (!comments.length) {
        box.innerHTML = `<div class="muted">No comments yet.</div>`;
        return;
      }
      box.innerHTML = comments.map((c) => `
        <div class="comment">
          <div><strong>${esc(c.author)}</strong> <span class="muted">${new Date(c.created_at * 1000).toLocaleString()}</span></div>
          <div style="white-space:pre-wrap;">${esc(c.body)}</div>
        </div>
      `).join("");
    }

    async function createPost() {
      const payload = {
        author: el("author").value.trim(),
        title: el("title").value.trim(),
        body: el("body").value.trim()
      };
      try {
        const post = await api(`/api/v1/frays/${state.fray}/posts`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload)
        });
        el("title").value = "";
        el("body").value = "";
        setStatus("Thread posted", "ok");
        await loadPosts();
        await loadThread(post.id);
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function createComment() {
      if (!state.postId) return;
      const payload = {
        author: el("comment-author").value.trim(),
        body: el("comment-body").value.trim()
      };
      try {
        await api(`/api/v1/frays/${state.fray}/posts/${state.postId}/comments`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload)
        });
        el("comment-body").value = "";
        setStatus("Comment added", "ok");
        await loadThread(state.postId);
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function syncPull() {
      try {
        const result = await api(`/api/v1/frays/${state.fray}/sync/pull`, { method: "POST" });
        setStatus(`Pulled ${result.imported_posts} objects from network`, "ok");
        await loadPosts();
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function syncPublish() {
      try {
        const result = await api(`/api/v1/frays/${state.fray}/sync/publish`, { method: "POST" });
        setStatus(`Published ${result.published_posts} posts and ${result.published_comments} comments`, "ok");
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    el("load").addEventListener("click", () => { state.postId = null; loadPosts(); });
    el("create").addEventListener("click", createPost);
    el("comment-create").addEventListener("click", createComment);
    el("sync-pull").addEventListener("click", syncPull);
    el("sync-publish").addEventListener("click", syncPublish);
    readRoute();
    loadPosts();
  </script>
</body>
</html>"#
}
