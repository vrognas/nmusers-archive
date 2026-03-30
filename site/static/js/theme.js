(function () {
  const STORAGE_KEY = "nmusers-theme";

  function getPreferred() {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) return stored;
    return "light";
  }

  function apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
  }

  // Apply immediately to avoid flash
  apply(getPreferred());

  document.addEventListener("DOMContentLoaded", function () {
    const btn = document.getElementById("theme-toggle");
    if (btn) {
      btn.addEventListener("click", function () {
        const current = document.documentElement.getAttribute("data-theme");
        const next = current === "dark" ? "light" : "dark";
        apply(next);
        localStorage.setItem(STORAGE_KEY, next);
      });
    }
  });
})();
