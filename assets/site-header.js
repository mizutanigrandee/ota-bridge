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
        <img src="${CFG.logo}" alt="${CFG.title} logo" decoding="async"/>
        <strong>${CFG.title}</strong>
      </a>
      <nav class="site-nav" role="navigation" aria-label="Main">
        ${CFG.menu.map(m => `<a href="${m.path}" data-mid="${m.id}">${m.label}</a>`).join("")}
      </nav>
    </div>
  `;

  const exist = document.querySelector("header.site-header");
  exist ? exist.replaceWith(hdr) : document.body.prepend(hdr);

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
