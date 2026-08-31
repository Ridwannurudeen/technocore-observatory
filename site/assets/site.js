(() => {
  const root = document.documentElement;
  const themeSelect = document.querySelector("#theme-select");
  const storedTheme = localStorage.getItem("observatory-theme");

  function applyTheme(theme) {
    if (theme === "light" || theme === "dark") {
      root.dataset.theme = theme;
    } else {
      delete root.dataset.theme;
    }
    if (themeSelect) themeSelect.value = theme;
  }

  applyTheme(storedTheme || "system");
  if (themeSelect) {
    themeSelect.addEventListener("change", () => {
      const theme = themeSelect.value;
      localStorage.setItem("observatory-theme", theme);
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
        ? `${count} characters · the submitted value is treated as an untrusted label.`
        : "";
    });
    search.addEventListener("submit", () => {
      feedback.textContent = "Opening the local evidence register…";
    });
  }
})();
