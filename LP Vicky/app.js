/* ====================================
   Vicky LP — interactions
   ==================================== */

(() => {
  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;

  /* ---------- Reveal on scroll ---------- */
  const revealEls = document.querySelectorAll(".reveal");
  if ("IntersectionObserver" in window && !reduced) {
    const io = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.classList.add("in");
          io.unobserve(entry.target);
        }
      });
    }, { threshold: 0.12, rootMargin: "0px 0px -40px 0px" });
    revealEls.forEach(el => io.observe(el));
  } else {
    revealEls.forEach(el => el.classList.add("in"));
  }

  /* ---------- Hero pipeline animation ---------- */
  const steps = document.querySelectorAll("#pmSteps .pm-step");
  const pctEl = document.getElementById("pmPct");
  const pcts = [4, 14, 28, 42, 56, 70, 86, 100];
  let activeIndex = 0;

  function tickPipeline() {
    steps.forEach((s, i) => {
      s.classList.remove("is-done", "is-active");
      if (i < activeIndex) s.classList.add("is-done");
      else if (i === activeIndex) s.classList.add("is-active");
    });
    if (pctEl) pctEl.textContent = pcts[activeIndex] + "%";
    activeIndex = (activeIndex + 1) % (steps.length + 1);
    if (activeIndex > steps.length - 1) {
      // brief "complete" pause before reset
      setTimeout(() => { activeIndex = 0; tickPipeline(); }, 1400);
      return;
    }
    setTimeout(tickPipeline, 1200);
  }
  if (steps.length && !reduced) tickPipeline();
  else steps.forEach((s, i) => i < 3 ? s.classList.add("is-done") : i === 3 && s.classList.add("is-active"));

  /* ---------- Tabs (Produto) ---------- */
  const tabs = document.querySelectorAll(".tab");
  const panels = document.querySelectorAll(".tab-panel");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const idx = tab.dataset.tab;
      tabs.forEach(t => { t.classList.remove("active"); t.setAttribute("aria-selected", "false"); });
      panels.forEach(p => p.classList.remove("active"));
      tab.classList.add("active");
      tab.setAttribute("aria-selected", "true");
      const panel = document.querySelector(`.tab-panel[data-panel="${idx}"]`);
      if (panel) panel.classList.add("active");
    });
  });

  /* ---------- FAQ accordion ---------- */
  const faqItems = document.querySelectorAll(".faq-item");
  faqItems.forEach(item => {
    const q = item.querySelector(".faq-q");
    const a = item.querySelector(".faq-a");
    q.addEventListener("click", () => {
      const open = item.classList.toggle("is-open");
      q.setAttribute("aria-expanded", open ? "true" : "false");
      if (open) {
        a.style.maxHeight = a.scrollHeight + "px";
      } else {
        a.style.maxHeight = "0px";
      }
    });
  });

  /* ---------- Number counters ---------- */
  const counters = document.querySelectorAll(".counter");
  const animateCounter = (el) => {
    const to = parseFloat(el.dataset.to);
    const suffix = el.dataset.suffix || "";
    const duration = 1500;
    const start = performance.now();
    const tick = (now) => {
      const t = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      const val = Math.round(to * eased);
      el.textContent = val + suffix;
      if (t < 1) requestAnimationFrame(tick);
      else el.textContent = to + suffix;
    };
    requestAnimationFrame(tick);
  };
  if ("IntersectionObserver" in window && !reduced) {
    const cio = new IntersectionObserver((entries) => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          animateCounter(e.target);
          cio.unobserve(e.target);
        }
      });
    }, { threshold: 0.5 });
    counters.forEach(c => cio.observe(c));
  } else {
    counters.forEach(c => c.textContent = c.dataset.to + (c.dataset.suffix || ""));
  }

  /* ---------- Smooth anchor offset for fixed nav ---------- */
  document.querySelectorAll('a[href^="#"]').forEach(a => {
    a.addEventListener("click", (e) => {
      const id = a.getAttribute("href");
      if (id.length < 2) return;
      const target = document.querySelector(id);
      if (!target) return;
      e.preventDefault();
      const y = target.getBoundingClientRect().top + window.scrollY - 70;
      window.scrollTo({ top: y, behavior: reduced ? "auto" : "smooth" });
    });
  });
})();
