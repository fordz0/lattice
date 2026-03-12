pub fn page_html() -> &'static str {
    r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fray</title>
  <style>
    :root {
      --bg: #0d1117;
      --surface: #161b22;
      --surface-alt: #11161d;
      --line: #30363d;
      --text: #e6edf3;
      --muted: #8b949e;
      --accent: #f78166;
      --accent-soft: #3d1f1a;
      --good: #3fb950;
      --warn: #d29922;
      --bad: #f85149;
      --input: #0d1117;
      --hover: #1c2128;
      --shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
    }

    body[data-theme="light"] {
      --bg: #f6f1e9;
      --surface: #fffaf3;
      --surface-alt: #f3e7d9;
      --line: #d8c7b2;
      --text: #1e2a33;
      --muted: #5f6d75;
      --accent: #cc5a21;
      --accent-soft: #f4d8c7;
      --good: #1f8a4d;
      --warn: #8a6c18;
      --bad: #a22929;
      --input: #f6f1e9;
      --hover: #f4ede4;
      --shadow: 0 18px 40px rgba(71, 48, 29, 0.12);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }

    a { color: inherit; text-decoration: none; }
    a:hover { text-decoration: underline; }

    button,
    input,
    textarea,
    select {
      font: inherit;
      color: inherit;
    }

    button,
    input,
    textarea,
    select {
      border: 1px solid var(--line);
      background: var(--input);
      border-radius: 8px;
    }

    input,
    textarea,
    select {
      width: 100%;
      padding: 10px 12px;
    }

    textarea {
      min-height: 120px;
      resize: vertical;
    }

    button {
      cursor: pointer;
      padding: 8px 12px;
      transition: background 120ms ease, border-color 120ms ease, color 120ms ease;
    }

    button:hover {
      filter: brightness(1.03);
    }

    .btn-primary {
      background: var(--accent);
      color: #fff;
      border-color: transparent;
      font-weight: 600;
    }

    .btn-ghost {
      background: transparent;
      color: var(--muted);
      border-color: var(--line);
      font-weight: 500;
    }

    .btn-ghost:hover {
      background: var(--hover);
      color: var(--text);
    }

    .wrap {
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 0 20px 28px;
    }

    header {
      position: sticky;
      top: 0;
      z-index: 30;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 16px 20px;
      margin: 0 -20px 20px;
      background: color-mix(in srgb, var(--surface) 94%, transparent);
      backdrop-filter: blur(18px);
      border-bottom: 1px solid var(--line);
    }

    .brandmark {
      display: grid;
      gap: 4px;
    }

    h1 {
      margin: 0;
      font-size: 28px;
      line-height: 1;
      font-weight: 700;
      letter-spacing: 0.02em;
    }

    .sub {
      color: var(--muted);
      font-size: 13px;
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .shell {
      display: grid;
      grid-template-columns: 240px minmax(0, 1fr);
      gap: 20px;
      align-items: start;
    }

    .sidebar,
    .card,
    .thread-shell {
      background: var(--surface);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }

    .sidebar {
      position: sticky;
      top: 86px;
      border-radius: 14px;
      overflow: hidden;
    }

    .sidebar-section {
      padding: 16px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 10px;
    }

    .sidebar-section:last-child {
      border-bottom: 0;
    }

    .wordmark {
      margin: 0;
      font-size: 24px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }

    .sidebar-label,
    .eyebrow {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .nav {
      display: grid;
      gap: 8px;
    }

    .nav button {
      width: 100%;
      text-align: left;
    }

    .main-column {
      min-width: 0;
      display: grid;
      gap: 16px;
    }

    .feed-topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
    }

    .title {
      margin: 0;
      font-size: 16px;
      font-weight: 700;
      letter-spacing: 0.01em;
    }

    .card {
      border-radius: 14px;
      padding: 16px;
    }

    .composer {
      display: grid;
      gap: 10px;
      margin-bottom: 14px;
    }

    .composer.hidden {
      display: none !important;
    }

    .posts {
      display: grid;
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      background: var(--surface);
    }

    .post {
      position: relative;
      display: grid;
      grid-template-columns: 28px minmax(0, 1fr);
      gap: 0 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: transparent;
    }

    .post:last-child {
      border-bottom: 0;
    }

    .post::before {
      content: "▲";
      display: flex;
      align-items: flex-start;
      justify-content: center;
      grid-row: 1 / span 2;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.8;
      opacity: 0.7;
    }

    .post:hover {
      background: var(--hover);
    }

    .post-title {
      margin: 0;
      font-size: 15px;
      font-weight: 500;
      color: var(--text);
      cursor: pointer;
    }

    .post > div:first-of-type {
      min-width: 0;
    }

    .post > .muted {
      grid-column: 2;
      font-size: 12px;
    }

    .post.restricted .post-title,
    .post.restricted .muted,
    .comment.restricted strong {
      color: var(--muted);
      font-style: italic;
    }

    .thread-shell {
      border-radius: 14px;
      padding: 16px;
    }

    .thread-shell h4 {
      margin: 18px 0 10px;
      font-size: 13px;
      color: var(--muted);
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    #thread-title {
      margin: 0 0 8px;
      font-size: 24px;
      line-height: 1.15;
    }

    #thread-body,
    .comment,
    .directory-entry,
    .trust-entry,
    .mod-entry {
      font-size: 14px;
    }

    #thread-body {
      white-space: pre-wrap;
      line-height: 1.6;
    }

    .comments,
    .directory-list,
    .trust-list,
    .mods-list {
      display: grid;
      gap: 12px;
    }

    .comment,
    .directory-entry,
    .trust-entry,
    .mod-entry {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px;
      background: var(--surface-alt);
    }

    .comment {
      border-left: 3px solid color-mix(in srgb, var(--accent) 65%, transparent);
    }

    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      line-height: 1;
      background: transparent;
      white-space: nowrap;
    }

    .chip.good {
      color: var(--good);
      border-color: color-mix(in srgb, var(--good) 55%, var(--line));
    }

    .chip.warn {
      color: var(--warn);
      border-color: color-mix(in srgb, var(--warn) 55%, var(--line));
    }

    .dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      display: inline-block;
      background: currentColor;
    }

    .muted {
      color: var(--muted);
      font-size: 12px;
    }

    .hidden {
      display: none !important;
    }

    .row {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .row > * {
      min-width: 0;
    }

    .stack {
      display: grid;
      gap: 10px;
    }

    .status {
      position: sticky;
      bottom: 16px;
      margin-top: 4px;
      min-height: 18px;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid transparent;
      background: transparent;
      color: var(--muted);
      font-size: 13px;
    }

    .status.ok {
      color: var(--good);
      background: color-mix(in srgb, var(--good) 10%, transparent);
      border-color: color-mix(in srgb, var(--good) 28%, transparent);
    }

    .status.err {
      color: var(--bad);
      background: color-mix(in srgb, var(--bad) 10%, transparent);
      border-color: color-mix(in srgb, var(--bad) 28%, transparent);
    }

    .restricted {
      color: var(--muted);
    }

    .collapsed-body {
      display: none;
      margin-top: 8px;
    }

    code {
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      word-break: break-all;
    }

    @media (max-width: 960px) {
      .shell {
        grid-template-columns: 1fr;
      }

      .sidebar {
        position: static;
      }

      header {
        flex-direction: column;
        align-items: stretch;
      }

      .toolbar {
        justify-content: flex-start;
      }
    }
  </style>
</head>
<body data-theme="dark">
  <div class="wrap">
    <header>
      <div class="brandmark">
        <h1>Fray</h1>
        <div class="sub">Distributed threads and local trust for Lattice</div>
      </div>
      <div class="toolbar">
        <button id="theme-toggle" class="btn-ghost" type="button" aria-label="Toggle theme">☀</button>
        <button id="sync-pull" class="btn-ghost" type="button">Pull Network Feed</button>
        <button id="sync-publish" class="btn-ghost" type="button">Publish Feed</button>
      </div>
    </header>

    <div class="shell">
      <aside class="sidebar">
        <section class="sidebar-section">
          <div class="eyebrow">Local app</div>
          <div class="wordmark">Fray</div>
          <div class="sub">Threads, directories, and trust records over Lattice.</div>
        </section>

        <section class="sidebar-section">
          <div class="sidebar-label">Current Fray</div>
          <input id="fray-name" placeholder="fray name (e.g. lattice)" value="lattice">
          <button id="load" class="btn-primary" type="button">Load /f/</button>
        </section>

        <section class="sidebar-section">
          <div class="sidebar-label">Navigate</div>
          <div class="nav">
            <button id="nav-home" class="btn-ghost" type="button">Feed</button>
            <button id="nav-directory" class="btn-ghost" type="button">Directory</button>
            <button id="nav-mod" class="btn-ghost" type="button">Mod Panel</button>
          </div>
        </section>

        <section class="sidebar-section">
          <div class="sidebar-label">Compose</div>
          <button id="new-thread-toggle" class="btn-primary" type="button">+ New Thread</button>
        </section>
      </aside>

      <main class="main-column">
        <section id="view-feed">
          <section class="card">
            <div class="feed-topbar">
              <div>
                <h2 class="title">Fray Feed</h2>
                <div class="muted">Browsing threads in the current fray.</div>
              </div>
              <button id="new-thread-inline" class="btn-ghost" type="button">+ New Thread</button>
            </div>

            <div id="new-thread-panel" class="card composer hidden">
              <h3 class="title">Create Thread</h3>
              <input id="author" placeholder="author (e.g. fordz0)" value="fordz0">
              <input id="title" placeholder="thread title">
              <textarea id="body" placeholder="write your thread..."></textarea>
              <div class="row">
                <button id="create" class="btn-primary" type="button">Post Thread</button>
                <button id="new-thread-close" class="btn-ghost" type="button">Close</button>
              </div>
            </div>

            <div id="posts" class="posts"></div>
          </section>

          <section class="thread-shell">
            <h2 class="title">Thread</h2>
            <div id="thread-empty" class="muted">Select a thread to view comments.</div>
            <article id="thread" style="display:none;">
              <h3 id="thread-title"></h3>
              <div id="thread-meta" class="muted"></div>
              <p id="thread-body"></p>
              <h4>Comments</h4>
              <div id="comments" class="comments"></div>
              <div class="stack" style="margin-top:14px;">
                <input id="comment-author" placeholder="comment author" value="fordz0">
                <textarea id="comment-body" placeholder="add a comment..."></textarea>
                <div class="row">
                  <button id="comment-create" class="btn-primary" type="button">Add Comment</button>
                </div>
              </div>
            </article>
          </section>
        </section>

        <section id="view-directory" class="card hidden">
          <h2 class="title">Directory</h2>
          <div class="muted">Listed frays published into the shared directory.</div>
          <div id="directory-list" class="directory-list" style="margin-top:12px;"></div>
        </section>

        <section id="view-mod" class="card hidden">
          <h2 class="title">Mod Panel</h2>
          <div id="mod-summary" class="muted">Load a fray you own to inspect its trust record.</div>
          <div id="mod-owned" class="stack hidden" style="margin-top:12px;">
            <div class="row">
              <input id="trust-key" placeholder="publisher key (base64)">
              <select id="trust-standing">
                <option>Trusted</option>
                <option selected>Normal</option>
                <option>Restricted</option>
              </select>
            </div>
            <input id="trust-label" placeholder="label (optional)">
            <input id="trust-reason" placeholder="reason (used for Restricted)">
            <div class="muted">Signed moderator actions are not wired into the browser yet. Use signed API requests locally for mutations.</div>
            <div class="trust-list" id="trust-list"></div>
            <h3 class="title" style="margin-top:8px;">Moderators</h3>
            <div class="mods-list" id="mods-list"></div>
          </div>
        </section>

        <div id="status" class="status"></div>
      </main>
    </div>
  </div>
  <script>
    const el = (id) => document.getElementById(id);
    const state = { view: "feed", fray: "lattice", postId: null, posts: [] };

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

    function routeTo() {
      if (state.view === "directory") {
        history.replaceState(null, "", "/directory");
      } else if (state.view === "mod") {
        history.replaceState(null, "", `/f/${state.fray}/mod`);
      } else if (state.postId) {
        history.replaceState(null, "", `/f/${state.fray}/${state.postId}`);
      } else {
        history.replaceState(null, "", `/f/${state.fray}`);
      }
    }

    function readRoute() {
      const parts = location.pathname.split("/").filter(Boolean);
      if (parts[0] === "directory") {
        state.view = "directory";
      } else if (parts[0] === "f" && parts[1] && parts[2] === "mod") {
        state.view = "mod";
        state.fray = parts[1].toLowerCase();
      } else if (parts[0] === "f" && parts[1]) {
        state.view = "feed";
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

    function standingMarkup(item) {
      if (item.standing === "Trusted") {
        return `<span class="chip good"><span class="dot"></span>Trusted</span>`;
      }
      if (item.standing === "Restricted") {
        return `<span class="chip warn">Restricted</span>`;
      }
      return "";
    }

    function maybeRestrictedBody(item, body, key) {
      if (item.standing !== "Restricted") {
        return `<div style="white-space:pre-wrap;">${esc(body)}</div>`;
      }
      const btn = `show-${key}`;
      return `
        <div class="restricted">This post is restricted locally.</div>
        <button class="btn-ghost" type="button" data-toggle="${btn}">show anyway</button>
        <div id="${btn}" class="collapsed-body" style="white-space:pre-wrap;">${esc(body)}</div>
      `;
    }

    async function loadView() {
      if (state.view === "directory") {
        await loadDirectory();
        return;
      }
      if (state.view === "mod") {
        await loadModPanel();
        return;
      }
      await loadPosts();
    }

    async function loadPosts() {
      state.fray = el("fray-name").value.trim().toLowerCase();
      if (!state.fray) return;
      state.view = "feed";
      toggleViews();
      setStatus("Loading feed...");
      try {
        const data = await api(`/api/v1/frays/${state.fray}/posts?limit=50`);
        state.posts = data.posts || [];
        renderPosts();
        setStatus(`Loaded ${state.posts.length} threads`, "ok");
        routeTo();
        if (state.postId) await loadThread(state.postId);
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    function renderPosts() {
      const posts = el("posts");
      if (!state.posts.length) {
        posts.innerHTML = `<div class="muted" style="padding:16px;">No threads yet in /f/${esc(state.fray)}</div>`;
        return;
      }
      posts.innerHTML = state.posts.map((p) => `
        <article class="post ${p.standing === "Restricted" ? "restricted" : ""}">
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:start;">
            <h3 class="post-title" data-id="${esc(p.id)}">${esc(p.title)}</h3>
            ${standingMarkup(p)}
          </div>
          <div class="muted">by ${esc(p.author)} · id ${esc(p.id.slice(0, 12))}</div>
        </article>
      `).join("");
      posts.querySelectorAll(".post-title").forEach((node) => {
        node.addEventListener("click", () => loadThread(node.dataset.id));
      });
    }

    async function loadThread(postId) {
      state.view = "feed";
      state.postId = postId;
      routeTo();
      try {
        const post = await api(`/api/v1/frays/${state.fray}/posts/${postId}`);
        const comments = await api(`/api/v1/frays/${state.fray}/posts/${postId}/comments?limit=200`);
        el("thread-empty").style.display = "none";
        el("thread").style.display = "block";
        el("thread-title").textContent = post.title;
        el("thread-meta").innerHTML = `by ${esc(post.author)} · ${new Date(post.created_at * 1000).toLocaleString()} ${standingMarkup(post)}`;
        el("thread-body").textContent = "";
        el("thread-body").innerHTML = post.standing === "Restricted"
          ? maybeRestrictedBody(post, post.body, `thread-${post.id}`)
          : esc(post.body);
        bindShowAnyway(el("thread-body"));
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
        <div class="comment ${c.standing === "Restricted" ? "restricted" : ""}">
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:start;">
            <div><strong>${esc(c.author)}</strong> <span class="muted">${new Date(c.created_at * 1000).toLocaleString()}</span></div>
            ${standingMarkup(c)}
          </div>
          ${maybeRestrictedBody(c, c.body, `comment-${c.id}`)}
        </div>
      `).join("");
      bindShowAnyway(box);
    }

    function bindShowAnyway(root) {
      root.querySelectorAll("[data-toggle]").forEach((button) => {
        button.addEventListener("click", () => {
          const target = el(button.dataset.toggle);
          if (!target) return;
          const hidden = target.style.display !== "block";
          target.style.display = hidden ? "block" : "none";
          button.textContent = hidden ? "hide again" : "show anyway";
        });
      });
    }

    async function loadDirectory() {
      state.view = "directory";
      state.postId = null;
      toggleViews();
      routeTo();
      setStatus("Loading directory...");
      try {
        const data = await api("/api/v1/directory");
        const entries = (data.directory && data.directory.entries) || data.entries || [];
        const listed = entries.filter((entry) => entry.status === "Listed" || (entry.status && entry.status.Listed !== undefined));
        const box = el("directory-list");
        if (!listed.length) {
          box.innerHTML = `<div class="muted">No listed frays in the cached directory.</div>`;
        } else {
          box.innerHTML = listed.map((entry) => `
            <article class="directory-entry">
              <div style="display:flex;justify-content:space-between;gap:10px;align-items:start;">
                <div>
                  <strong><a href="/f/${esc(entry.fray_name)}">${esc(entry.fray_name)}</a></strong>
                  <div class="muted">${esc(entry.description || "No description")}</div>
                </div>
                <span class="chip">${esc(typeof entry.status === "string" ? entry.status : Object.keys(entry.status || {})[0] || "Listed")}</span>
              </div>
              <div class="muted" style="margin-top:6px;">Owner <code>${esc(entry.owner_key_b64 || "")}</code></div>
            </article>
          `).join("");
        }
        setStatus("Directory loaded", "ok");
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function loadModPanel() {
      state.view = "mod";
      state.postId = null;
      toggleViews();
      routeTo();
      setStatus("Loading mod panel...");
      try {
        const trust = await api(`/api/v1/frays/${state.fray}/trust`);
        el("mod-summary").textContent = `Trust record v${trust.record.version} generated ${new Date(trust.record.generated_at * 1000).toLocaleString()}`;
        el("mod-owned").classList.remove("hidden");
        const entries = trust.record.entries || [];
        el("trust-list").innerHTML = entries.length
          ? entries.map((entry) => `
              <div class="trust-entry">
                <div style="display:flex;justify-content:space-between;gap:10px;align-items:start;">
                  <code>${esc(entry.key_b64)}</code>
                  <span class="chip">${esc(typeof entry.standing === "string" ? entry.standing : Object.keys(entry.standing || {})[0])}</span>
                </div>
                <div class="muted">${esc(entry.label || "")}</div>
              </div>
            `).join("")
          : `<div class="muted">No trust entries yet.</div>`;
        const moderators = trust.record.moderator_keys || [];
        el("mods-list").innerHTML = moderators.length
          ? moderators.map((key) => `<div class="mod-entry"><code>${esc(key)}</code></div>`).join("")
          : `<div class="muted">No moderators configured.</div>`;
        setStatus("Mod panel loaded", "ok");
      } catch (err) {
        el("mod-owned").classList.add("hidden");
        el("mod-summary").textContent = err.message;
        setStatus(err.message, "err");
      }
    }

    function toggleViews() {
      el("view-feed").classList.toggle("hidden", state.view !== "feed");
      el("view-directory").classList.toggle("hidden", state.view !== "directory");
      el("view-mod").classList.toggle("hidden", state.view !== "mod");
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
        closeComposer();
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
        if (state.view === "feed") await loadPosts();
        if (state.view === "mod") await loadModPanel();
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

    function applyTheme(theme) {
      document.body.dataset.theme = theme;
      el("theme-toggle").textContent = theme === "dark" ? "☀" : "☽";
      localStorage.setItem("fray-theme", theme);
    }

    function openComposer() {
      el("new-thread-panel").classList.remove("hidden");
      el("title").focus();
    }

    function closeComposer() {
      el("new-thread-panel").classList.add("hidden");
    }

    el("nav-home").addEventListener("click", () => { state.view = "feed"; loadPosts(); });
    el("nav-directory").addEventListener("click", () => { loadDirectory(); });
    el("nav-mod").addEventListener("click", () => { state.view = "mod"; loadModPanel(); });
    el("load").addEventListener("click", () => { state.postId = null; loadPosts(); });
    el("create").addEventListener("click", createPost);
    el("comment-create").addEventListener("click", createComment);
    el("sync-pull").addEventListener("click", syncPull);
    el("sync-publish").addEventListener("click", syncPublish);
    el("new-thread-toggle").addEventListener("click", openComposer);
    el("new-thread-inline").addEventListener("click", openComposer);
    el("new-thread-close").addEventListener("click", closeComposer);
    el("theme-toggle").addEventListener("click", () => {
      applyTheme(document.body.dataset.theme === "dark" ? "light" : "dark");
    });
    applyTheme(localStorage.getItem("fray-theme") || "dark");
    readRoute();
    toggleViews();
    loadView();
  </script>
</body>
</html>"#
}
