// assets/site-header.js — 共通ヘッダー + モバイルメニュー（確実動作版）
(() => {
  const script = document.currentScript;
  const active = script?.dataset?.active || "";

  // ▼ 必要に応じてパスだけ各リポジトリ用に調整してください
  const CFG = {
    logo: "assets/バナー画像3.png",
    title: "めちゃいいツール",
    menu: [
      // 本家 vacancy-dashboard 側なら：
      // { id: "calendar",  label: "料金カレンダー", path: "https://mizutanigrandee.github.io/vacancy-dashboard/" },
      // { id: "reviews",   label: "クチコミ比較",   path: "https://mizutanigrandee.github.io/ota-bridge/reviews.html" },
      // { id: "compete",   label: "競合比較",       path: "https://mizutanigrandee.github.io/ota-bridge/daily_preview.html" }

      // ota-bridge 側なら：
      // { id: "calendar",  label: "料金カレンダー", path: "https://mizutanigrandee.github.io/vacancy-dashboard/" },
      // { id: "reviews",   label: "クチコミ比較",   path: "reviews.html" },
      // { id: "compete",   label: "競合比較",       path: "daily_preview.html" }
    ]
  };

  // すでにメニューが空のままにならないよう、デフォルトを安全に補う
  if (!CFG.menu.length) {
    CFG.menu = [
      { id: "calendar",  label: "料金カレンダー", path: "index.html" },
      { id: "reviews",   label: "クチコミ比較",   path: "reviews.html" },
      { id: "compete",   label: "競合比較",       path: "daily_preview.html" }
    ];
  }

  const hdr = document.createElement("header");
  hdr.className = "site-header";
  hdr.innerHTML = `
    <div class="wrap">
      <a class="brand" href="${CFG.menu[0].path}">
        <img src="${CFG.logo}" alt="${CFG.title} logo" decoding="async" />
        <strong>${CFG.title}</strong>
      </a>

      <button class="menu-toggle" aria-label="メニュー" aria-expanded="false">☰</button>

      <nav class="site-nav" role="navigation" aria-label="Main">
        ${CFG.menu.map(m => `<a href="${m.path}" data-mid="${m.id}">${m.label}</a>`).join("")}
      </nav>
    </div>
  `;

  const exist = document.querySelector("header.site-header");
  if (exist) exist.replaceWith(hdr); else document.body.prepend(hdr);

  // アクティブ表示
  const here = (location.pathname || "").toLowerCase();
  hdr.querySelectorAll(".site-nav a").forEach(a => {
    const id = a.getAttribute("data-mid");
    const href = (a.getAttribute("href") || "").toLowerCase();
    if (active === id) {
      a.classList.add("is-active");
      a.setAttribute("aria-current", "page");
    } else if (href && here.endsWith(href.split("/").pop())) {
      a.classList.add("is-active");
      a.setAttribute("aria-current","page");
    }
  });

  // --- モバイルメニュー（リンク遷移と競合しない版） ---
  const toggle = hdr.querySelector(".menu-toggle");
  const navEl  = hdr.querySelector(".site-nav");
  if (toggle && navEl) {
    const OPEN = "open";
    const openMenu  = () => { navEl.classList.add(OPEN);  toggle.setAttribute("aria-expanded","true");  };
    const closeMenu = () => { navEl.classList.remove(OPEN); toggle.setAttribute("aria-expanded","false"); };

    const onToggle = (e) => {
      e.stopPropagation();
      if (navEl.classList.contains(OPEN)) closeMenu(); else openMenu();
    };
    toggle.addEventListener("pointerup", onToggle);
    toggle.addEventListener("click", onToggle); // 保険

    // 外側タップで閉じる（遷移を阻害しないよう次フレームで閉じる）
    const onOutside = (e) => {
      if (!hdr.contains(e.target)) setTimeout(closeMenu, 0);
    };
    document.addEventListener("pointerdown", onOutside, {passive:true});

    // a要素に closeMenu をバインドしない（遷移を優先）
    // もし過去のコードで付けていたら、下記のような行は削除してください：
    // navEl.querySelectorAll("a").forEach(a => a.addEventListener("click", closeMenu));
    // navEl.querySelectorAll("a").forEach(a => a.addEventListener("touchstart", closeMenu, {passive:true}));
  }
})();
