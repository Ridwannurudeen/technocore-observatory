(() => {
  const root = document.documentElement;
  const themeToggle = document.querySelector("#theme-toggle");
  const themes = ["system", "light", "dark"];

  function readTheme() {
    try {
      const stored = localStorage.getItem("observatory-theme");
      return themes.includes(stored) ? stored : "system";
    } catch {
      return "system";
    }
  }

  function applyTheme(theme) {
    if (theme === "light" || theme === "dark") {
      root.dataset.theme = theme;
    } else {
      root.removeAttribute("data-theme");
    }

    if (themeToggle) {
      const label = theme === "system" ? "auto" : theme;
      themeToggle.textContent = `THEME ${label.toUpperCase()}`;
      themeToggle.dataset.themeValue = theme;
    }
  }

  let theme = readTheme();
  applyTheme(theme);

  const page = document.body.dataset.page;
  if (page) {
    document.querySelectorAll(".priority-nav a").forEach((link) => {
      if (link.getAttribute("href") === `/${page}/`) {
        link.setAttribute("aria-current", "page");
      }
    });
  }

  if (themeToggle) {
    themeToggle.hidden = false;
    themeToggle.addEventListener("click", () => {
      const nextIndex = (themes.indexOf(theme) + 1) % themes.length;
      theme = themes[nextIndex];

      try {
        localStorage.setItem("observatory-theme", theme);
      } catch {}

      applyTheme(theme);
    });
  }

  document.querySelectorAll("[data-freshness-for]").forEach((word) => {
    const validity = document.getElementById(word.dataset.freshnessFor);
    if (!validity) return;
    const expiry = Date.parse(validity.getAttribute("datetime"));
    if (!Number.isFinite(expiry) || Date.now() <= expiry) return;
    const printed = word.textContent.trim();
    word.textContent = printed === printed.toLowerCase() ? "stale" : "Stale";
  });

  const search = document.querySelector(".room-search");
  const query = document.querySelector("#room-query");
  const feedback = document.querySelector("#search-feedback");

  if (search && query && feedback) {
    query.addEventListener("input", () => {
      const count = query.value.trim().length;
      feedback.textContent = count
        ? `${count} characters · submitted as an untrusted label.`
        : "";
    });

    search.addEventListener("submit", () => {
      feedback.textContent = "Opening the local evidence register.";
    });
  }
})();