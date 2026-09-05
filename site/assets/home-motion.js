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
      });
    },
    { threshold: 0.12 },
  );

  observer.observe(frame);

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
