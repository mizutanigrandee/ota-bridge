// assets/site-header.js — 共通ヘッダー + モバイルメニュー（自動判別版）
(() => {
  const script = document.currentScript;
  const active = script?.dataset?.active || "";

  // ここだけあなたの公開URLに合わせて確認
  const BASE = {
    vac: "https://mizutanigrandee.github.io/vacancy-dashboard/",
    ota: "https://mizutanigrandee.github.io/ota-bridge/"
  };

  const path = (location.pathname || "").toLowerCase();
  const isVac = path.includes("/vacancy-dashboard/");
  const isOta = path.includes("/ota-bridge/");

  // どのページでも正しいリンクになるよう自動生成
  const MENU = [
    { id: "calendar",  label: "料金カレンダー", path: BASE.vac },
    { id: "reviews",   label: "クチコミ比較",   path: BASE.ota + "reviews.html" },
    { id: "compete",   label: "競合比較",       path: BASE.ota + "daily_preview.html" }
  ];

  const CFG = {
    logo: "assets/バナー画像3.png",   // 両リポに同名で置いてある想定
    title: "めちゃいいツール",
    menu: MENU
  };

  // ヘッダー描画
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
  const hereUrl = (location.href || "").toLowerCase();
  hdr.querySelectorAll(".site-nav a").forEach(a => {
    const id = a.getAttribute("data-mid");
    const href = (a.getAttribute("href") || "").toLowerCase();
    const isMatch = active === id || hereUrl.startsWith(href);
    if (isMatch) { a.classList.add("is-active"); a.setAttribute("aria-current","page"); }
  });

  // --- モバイルメニュー（リンク遷移優先版） ---
  const toggle = hdr.querySelector(".menu-toggle");
  const navEl  = hdr.querySelector(".site-nav");
  if (toggle && navEl) {
    const OPEN = "open";
    const openMenu  = () => { navEl.classList.add(OPEN);  toggle.setAttribute("aria-expanded","true");  };
    const closeMenu = () => { navEl.classList.remove(OPEN); toggle.setAttribute("aria-expanded","false"); };

    const onToggle = (e) => { e.stopPropagation(); navEl.classList.toggle(OPEN); 
      toggle.setAttribute("aria-expanded", navEl.classList.contains(OPEN) ? "true" : "false"); };
    toggle.addEventListener("pointerup", onToggle);
    toggle.addEventListener("click", onToggle);

    // 外側タップで閉じる（遷移は阻害しない）
    const onOutside = (e) => { if (!hdr.contains(e.target)) setTimeout(closeMenu, 0); };
    document.addEventListener("pointerdown", onOutside, {passive:true});
  }
})();
