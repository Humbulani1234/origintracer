document.addEventListener("DOMContentLoaded", () => {
  const title = document.querySelector(".md-header__topic .md-ellipsis");
  if (title && title.textContent.startsWith("Stack")) {
    title.innerHTML = title.textContent.replace(
      "Stack", '<span style="color:#e8a020">Stack</span>'
    );
  }
});
