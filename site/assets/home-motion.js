(() => {
  const preference = window.matchMedia("(prefers-reduced-motion: reduce)");
  if (
    preference.matches ||
    !window.Motion ||
    !Element.prototype.animate ||
    !window.IntersectionObserver
  ) {
    return;
  }

  const frame = document.querySelector(".home-reading-frame");
  const controls = [];
  let stopped = false;
  const observer = new IntersectionObserver(
    (entries) => {
      if (stopped) return;
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        observer.unobserve(entry.target);

        if (entry.target === frame) {
          frame.style.opacity = "1";
          const traces = Array.from(
            frame.querySelectorAll(".home-frame-edge"),
            (edge, index) => {
              const scale = index % 2 === 0 ? "scaleX" : "scaleY";
              return window.Motion.animate(
                edge,
                { transform: [`${scale}(0)`, `${scale}(1)`] },
                { duration: 0.36, delay: index * 0.26, ease: "easeInOut" },
              );
            },
          );
          controls.push(...traces);
          Promise.all(traces.map((control) => control.finished)).then(() => {
            frame.style.opacity = "0";
          });
        } else {
          controls.push(
            window.Motion.animate(
              entry.target,
              {
                opacity: [0.92, 1],
                transform: ["translateY(12px)", "translateY(0px)"],
              },
              { duration: 0.45, ease: [0.22, 1, 0.36, 1] },
            ),
          );
        }
      });
    },
    { threshold: 0.12 },
  );

  observer.observe(frame);
  document
    .querySelectorAll(
      ".home-retention > div:first-child, .home-section-heading, " +
        ".home-history-links > a, .home-api > div:first-child, " +
        ".home-api-example, .home-boundary",
    )
    .forEach((section) => observer.observe(section));

  function finishMotion() {
    stopped = true;
    observer.disconnect();
    controls.forEach((control) => control.complete());
  }

  preference.addEventListener("change", (event) => {
    if (event.matches) finishMotion();
  });
  window.addEventListener("beforeprint", finishMotion);
})();
