// main.js — shared JS loaded on every page
document.addEventListener("DOMContentLoaded", () => {
  // Update favorites badge in navbar
  const favorites = JSON.parse(localStorage.getItem("favorites") || "[]");
  const badge = document.getElementById("favCount");
  if (badge && favorites.length > 0) {
    badge.style.display = "inline-flex";
    badge.textContent = favorites.length;
  }
});
