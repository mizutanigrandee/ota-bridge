// assets/site-header.js
(() => {
  const script = document.currentScript;
  const active = script?.dataset?.active || "";

  const CFG = {
    logo: "assets/バナー画像3.png",  // ロゴファイル名は後で差し替え可
    title: "めちゃいいツール",
    menu: [
      { id: "calendar",  label: "料金カレンダー", path: "https://mizutanigrandee.github.io/vacancy-dashboard/" },
      { id: "reviews",   label: "クチコミ比較",   path: "reviews.html" },
      { id: "compete",   label: "競合比較",       path: "daily_preview.html" }
    ]
  };

  const hdr = document.createElement("header");
  hdr.className = "site-header";
  hdr.innerHTML = `
    <div class="wrap">
      <a class="brand" href="${CFG.menu[0].path}">
        <img src="${CFG.logo}" alt="${CFG.title} logo" decoding="async" />
        <strong>${CFG.title}</strong>
      </a>

      <!-- ▼ ここが追加：ハンバーガーボタン -->
      <button class="menu-toggle" aria-label="メニュー">☰</button>

      <nav class="site-nav" role="navigation" aria-label="Main">
        ${CFG.menu.map(m => `<a href="${m.path}" data-mid="${m.id}">${m.label}</a>`).join("")}
      </nav>
    </div>
  `;


  const exist = document.querySelector("header.site-header");
  exist ? exist.replaceWith(hdr) : document.body.prepend(hdr);

  // ▼ ハンバーガー開閉（iOS Safari 対応: touchstart と click の両方を拾う）
  const toggle = hdr.querySelector(".menu-toggle");
  const navEl = hdr.querySelector(".site-nav");

  if (toggle && navEl) {
    const openClass = "open";
    const openMenu = () => { navEl.classList.add(openClass);  toggle.setAttribute("aria-expanded","true");  };
    const closeMenu = () => { navEl.classList.remove(openClass); toggle.setAttribute("aria-expanded","false"); };

    const onToggle = (e) => {
      // タップとクリックが両方飛ぶ端末向けの二重起動防止
      if (e.type === "touchstart") e.preventDefault();
      e.stopPropagation();
      if (navEl.classList.contains(openClass)) {
        closeMenu();
      } else {
        openMenu();
      }
    };

    toggle.setAttribute("aria-expanded","false");
    toggle.addEventListener("touchstart", onToggle, {passive:false});
    toggle.addEventListener("click", onToggle);

    // メニュー外クリックで閉じる
    document.addEventListener("click", (e) => {
      if (!hdr.contains(e.target)) closeMenu();
    });
    document.addEventListener("touchstart", (e) => {
      if (!hdr.contains(e.target)) closeMenu();
    }, {passive:true});

    // メニュー内リンクを押したら閉じる
    navEl.querySelectorAll("a").forEach(a => {
      a.addEventListener("click", closeMenu);
      a.addEventListener("touchstart", closeMenu, {passive:true});
    });
  }



  const hereFile = (location.pathname.split("/").pop() || "").toLowerCase();
  document.querySelectorAll(".site-nav a").forEach(a => {
    const id = a.getAttribute("data-mid");
    const file = (a.getAttribute("href") || "").split("/").pop().toLowerCase();
    if (active === id || file === hereFile) {
      a.classList.add("is-active");
      a.setAttribute("aria-current","page");
    }
  });
})();

// === スマホ用ハンバーガーメニュー ===
document.addEventListener("DOMContentLoaded", ()=>{
  const toggle = document.querySelector(".menu-toggle");
  const nav = document.querySelector(".site-nav");

  if(toggle && nav){
    toggle.addEventListener("click", ()=>{
      nav.classList.toggle("open");
    });
  }
});
