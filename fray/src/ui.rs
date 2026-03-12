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

    .identity-shell {
      position: relative;
    }

    .identity-button {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      max-width: 240px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .identity-dropdown {
      position: absolute;
      right: 0;
      top: calc(100% + 8px);
      width: 320px;
      padding: 14px;
      border-radius: 12px;
      background: var(--surface);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      display: grid;
      gap: 10px;
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

    .banner {
      margin-bottom: 16px;
      padding: 12px 14px;
      border: 1px solid color-mix(in srgb, var(--warn) 45%, var(--line));
      border-radius: 12px;
      background: color-mix(in srgb, var(--warn) 10%, transparent);
      color: var(--text);
    }

    .modal-backdrop {
      position: fixed;
      inset: 0;
      z-index: 60;
      display: grid;
      place-items: center;
      padding: 20px;
      background: rgba(1, 4, 9, 0.72);
      backdrop-filter: blur(8px);
    }

    .modal-card {
      width: min(560px, 100%);
      padding: 20px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: var(--surface);
      box-shadow: var(--shadow);
      display: grid;
      gap: 12px;
    }

    .inline-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }

    .profile-grid {
      display: grid;
      gap: 10px;
    }

    .input-locked {
      background: var(--surface-alt);
      color: var(--muted);
      border-color: var(--line);
    }

    .input-locked:focus {
      outline: none;
      border-color: var(--line);
      box-shadow: none;
    }

    .field-note {
      margin-top: -6px;
      color: var(--muted);
      font-size: 12px;
    }

    .author-mark {
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }

    .verify {
      color: var(--good);
      font-size: 12px;
      font-weight: 700;
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
        <div class="identity-shell">
          <button id="identity-toggle" class="btn-ghost identity-button" type="button">anonymous 🔑</button>
          <div id="identity-dropdown" class="identity-dropdown hidden">
            <div>
              <div class="eyebrow">Node identity</div>
              <div id="identity-handle" class="title">anonymous</div>
              <div id="identity-display" class="muted"></div>
            </div>
            <div id="identity-bio" class="muted"></div>
            <div class="muted">Key <code id="identity-key"></code></div>
            <div class="inline-actions">
              <button id="identity-edit" class="btn-primary" type="button">Edit Profile</button>
            </div>
          </div>
        </div>
      </div>
    </header>

    <div id="identity-banner" class="banner hidden">
      No handle claimed — your posts will be anonymous.
    </div>

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
          <div class="card" style="margin-top:12px;">
            <h3 class="title">Publish Directory Entry</h3>
            <div class="stack">
              <select id="directory-status">
                <option selected>Listed</option>
                <option>Unlisted</option>
                <option>Banned</option>
              </select>
              <input id="directory-description" placeholder="short description">
              <div class="inline-actions">
                <button id="directory-publish" class="btn-primary" type="button">Publish Entry</button>
              </div>
            </div>
          </div>
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
            <div class="muted">Moderator actions are signed locally by your node before they are published.</div>
            <div class="inline-actions">
              <button id="trust-save" class="btn-primary" type="button">Save Standing</button>
            </div>
            <div class="trust-list" id="trust-list"></div>
            <h3 class="title" style="margin-top:8px;">Moderators</h3>
            <div class="inline-actions">
              <input id="moderator-key" placeholder="moderator key (base64)">
              <button id="moderator-add" class="btn-primary" type="button">Add Moderator</button>
            </div>
            <div class="mods-list" id="mods-list"></div>
            <h3 class="title" style="margin-top:8px;">Admin Blocklist</h3>
            <div class="inline-actions">
              <input id="blocklist-hash" placeholder="blake3 body hash">
              <button id="blocklist-add" class="btn-primary" type="button">Block Hash</button>
            </div>
          </div>
        </section>

        <div id="status" class="status"></div>
      </main>
    </div>
  </div>

  <div id="onboarding-modal" class="modal-backdrop hidden">
    <div class="modal-card">
      <div>
        <h2 id="identity-modal-title" class="title">Claim your handle</h2>
        <div id="identity-modal-copy" class="muted">Your handle is tied to your node key and claimed on the Lattice network. Once claimed, only you can use it.</div>
      </div>
      <input id="onboarding-handle" placeholder="handle">
      <div id="onboarding-handle-note" class="field-note hidden">handle cannot be changed</div>
      <input id="onboarding-display-name" placeholder="display name (optional)">
      <textarea id="onboarding-bio" placeholder="bio (optional)"></textarea>
      <div id="onboarding-error" class="status err"></div>
      <div class="inline-actions">
        <button id="onboarding-claim" class="btn-primary" type="button">Claim Handle</button>
        <button id="onboarding-skip" class="btn-ghost" type="button">skip for now</button>
      </div>
    </div>
  </div>
  <script>
    const el = (id) => document.getElementById(id);
    const state = {
      view: "feed",
      fray: "lattice",
      postId: null,
      posts: [],
      identity: null,
      identityModalMode: "onboarding",
      onboardingSkipped: localStorage.getItem("fray-handle-skipped") === "1"
    };

    function setStatus(message, kind) {
      const node = el("status");
      node.textContent = message || "";
      node.className = "status" + (kind ? " " + kind : "");
    }

    function localAuthor() {
      return state.identity && state.identity.handle ? state.identity.handle : "anonymous";
    }

    function esc(text) {
      return String(text || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    function authorMarkup(item) {
      const verified = item.verified ? `<span class="verify">✓</span>` : "";
      return `<span class="author-mark">${esc(item.author)}${verified}</span>`;
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

    async function signedApi(path, body) {
      const raw = JSON.stringify(body);
      const signed = await api("/api/v1/sign", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: raw
      });
      const headers = {
        "content-type": "application/json",
        "X-Fray-Signature": signed.signature_b64
      };
      return api(path, { method: "POST", headers, body: raw });
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
          <div class="muted">by ${authorMarkup(p)} · id ${esc(p.id.slice(0, 12))}</div>
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
        el("thread-meta").innerHTML = `by ${authorMarkup(post)} · ${new Date(post.created_at * 1000).toLocaleString()} ${standingMarkup(post)}`;
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
            <div><strong>${authorMarkup(c)}</strong> <span class="muted">${new Date(c.created_at * 1000).toLocaleString()}</span></div>
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
        author: localAuthor(),
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
        author: localAuthor(),
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

    function renderIdentity() {
      const identity = state.identity;
      const hasHandle = identity && identity.handle;
      const label = hasHandle ? `${identity.handle} 🔑` : "anonymous 🔑";
      el("identity-toggle").textContent = label;
      el("identity-handle").textContent = hasHandle ? identity.handle : "anonymous";
      el("identity-display").textContent = identity && identity.display_name ? identity.display_name : "No display name set";
      el("identity-bio").textContent = identity && identity.bio ? identity.bio : "No bio set";
      el("identity-key").textContent = identity && identity.key_b64 ? `${identity.key_b64.slice(0, 16)}...` : "";
      el("identity-banner").classList.toggle("hidden", !!hasHandle || !state.onboardingSkipped);
    }

    function setIdentityModalMode(mode) {
      const editing = mode === "edit";
      state.identityModalMode = mode;
      el("identity-modal-title").textContent = editing ? "Edit profile" : "Claim your handle";
      el("identity-modal-copy").textContent = editing
        ? "Your handle is tied to your node key and cannot be changed. You can still update your display name and bio."
        : "Your handle is tied to your node key and claimed on the Lattice network. Once claimed, only you can use it.";
      el("onboarding-handle").readOnly = editing;
      el("onboarding-handle").classList.toggle("input-locked", editing);
      el("onboarding-handle-note").classList.toggle("hidden", !editing);
      el("onboarding-claim").textContent = editing ? "Save Profile" : "Claim Handle";
      el("onboarding-skip").classList.toggle("hidden", editing);
    }

    async function loadIdentity() {
      try {
        state.identity = await api("/api/v1/identity");
      } catch (_) {
        state.identity = null;
      }
      renderIdentity();
      const shouldPrompt = !state.identity || !state.identity.handle;
      setIdentityModalMode("onboarding");
      el("onboarding-modal").classList.toggle("hidden", !shouldPrompt || state.onboardingSkipped);
    }

    async function submitHandleClaim(handleOverride) {
      const body = {
        handle: (handleOverride || el("onboarding-handle").value).trim().toLowerCase(),
        display_name: el("onboarding-display-name").value.trim() || null,
        bio: el("onboarding-bio").value.trim() || null
      };
      if (!/^[a-z0-9_-]{1,32}$/.test(body.handle)) {
        el("onboarding-error").textContent = "handle must be 1-32 chars: lowercase letters, digits, hyphens, underscores";
        return;
      }
      try {
        await api("/api/v1/identity/claim", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body)
        });
        state.onboardingSkipped = false;
        localStorage.removeItem("fray-handle-skipped");
        el("onboarding-error").textContent = "";
        el("onboarding-modal").classList.add("hidden");
        await loadIdentity();
        setStatus(`claimed @${body.handle}`, "ok");
      } catch (err) {
        el("onboarding-error").textContent = err.message;
      }
    }

    async function publishDirectoryEntry() {
      if (!state.identity || !state.identity.key_b64) {
        setStatus("load identity first", "err");
        return;
      }
      try {
        await signedApi("/api/v1/directory/entries", {
          fray_name: state.fray,
          owner_key_b64: state.identity.key_b64,
          status: el("directory-status").value,
          description: el("directory-description").value.trim() || null
        });
        setStatus("directory entry published", "ok");
        await loadDirectory();
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function saveStanding() {
      try {
        await signedApi(`/api/v1/frays/${state.fray}/trust/standings`, {
          key_b64: el("trust-key").value.trim(),
          standing: el("trust-standing").value,
          label: el("trust-label").value.trim() || null,
          reason: el("trust-reason").value.trim() || null
        });
        setStatus("trust standing updated", "ok");
        await loadModPanel();
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function addModerator() {
      try {
        await signedApi(`/api/v1/frays/${state.fray}/trust/moderators`, {
          key_b64: el("moderator-key").value.trim()
        });
        el("moderator-key").value = "";
        setStatus("moderator added", "ok");
        await loadModPanel();
      } catch (err) {
        setStatus(err.message, "err");
      }
    }

    async function addBlockHash() {
      try {
        await signedApi("/api/v1/admin/blocklist", {
          hash_hex: el("blocklist-hash").value.trim()
        });
        el("blocklist-hash").value = "";
        setStatus("hash blocklisted", "ok");
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

    el("identity-toggle").addEventListener("click", () => {
      el("identity-dropdown").classList.toggle("hidden");
    });
    el("identity-edit").addEventListener("click", () => {
      setIdentityModalMode("edit");
      el("onboarding-handle").value = state.identity && state.identity.handle ? state.identity.handle : "";
      el("onboarding-display-name").value = state.identity && state.identity.display_name ? state.identity.display_name : "";
      el("onboarding-bio").value = state.identity && state.identity.bio ? state.identity.bio : "";
      el("onboarding-error").textContent = "";
      el("onboarding-modal").classList.remove("hidden");
    });
    el("nav-home").addEventListener("click", () => { state.view = "feed"; loadPosts(); });
    el("nav-directory").addEventListener("click", () => { loadDirectory(); });
    el("nav-mod").addEventListener("click", () => { state.view = "mod"; loadModPanel(); });
    el("load").addEventListener("click", () => { state.postId = null; loadPosts(); });
    el("create").addEventListener("click", createPost);
    el("comment-create").addEventListener("click", createComment);
    el("sync-pull").addEventListener("click", syncPull);
    el("sync-publish").addEventListener("click", syncPublish);
    el("directory-publish").addEventListener("click", publishDirectoryEntry);
    el("trust-save").addEventListener("click", saveStanding);
    el("moderator-add").addEventListener("click", addModerator);
    el("blocklist-add").addEventListener("click", addBlockHash);
    el("new-thread-toggle").addEventListener("click", openComposer);
    el("new-thread-inline").addEventListener("click", openComposer);
    el("new-thread-close").addEventListener("click", closeComposer);
    el("onboarding-claim").addEventListener("click", () => submitHandleClaim());
    el("onboarding-skip").addEventListener("click", () => {
      state.onboardingSkipped = true;
      localStorage.setItem("fray-handle-skipped", "1");
      el("onboarding-modal").classList.add("hidden");
      renderIdentity();
    });
    el("theme-toggle").addEventListener("click", () => {
      applyTheme(document.body.dataset.theme === "dark" ? "light" : "dark");
    });
    applyTheme(localStorage.getItem("fray-theme") || "dark");
    readRoute();
    toggleViews();
    loadIdentity().then(loadView);
  </script>
</body>
</html>"#
}
