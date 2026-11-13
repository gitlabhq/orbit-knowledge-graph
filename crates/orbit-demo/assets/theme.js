"use strict";

(function () {
  const KEY = "orbit-demo-theme";
  let saved = null;
  try { saved = localStorage.getItem(KEY); } catch (_) { saved = null; }
  const system = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  document.documentElement.dataset.theme = saved === "dark" || saved === "light" ? saved : system;

  window.Theme = {
    current: () => (document.documentElement.dataset.theme === "dark" ? "dark" : "light"),
    toggle() {
      const next = this.current() === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      try { localStorage.setItem(KEY, next); } catch (_) { return next; }
      return next;
    },
  };
})();
