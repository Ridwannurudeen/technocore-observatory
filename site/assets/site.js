(() => {
  const root = document.documentElement;
  const themeToggle = document.querySelector("#theme-toggle");
  const themes = ["system", "light", "dark"];

  function readTheme() {
    try {
      const stored = localStorage.getItem("observatory-theme");
      return themes.includes(stored) ? stored : "system";
    } catch {
      root.dataset.themeStorage = "unavailable";
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
      themeToggle.setAttribute("aria-label", `Theme: ${label}`);
      themeToggle.dataset.themeValue = theme;
    }
  }

  let theme = readTheme();
  applyTheme(theme);

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const nextIndex = (themes.indexOf(theme) + 1) % themes.length;
      theme = themes[nextIndex];

      try {
        localStorage.setItem("observatory-theme", theme);
      } catch {
        root.dataset.themeStorage = "unavailable";
      }

      applyTheme(theme);
    });
  }

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