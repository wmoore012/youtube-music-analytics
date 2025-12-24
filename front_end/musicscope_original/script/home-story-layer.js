import Chart from "chart.js/auto";

const STORY_OVERVIEW_URL = "/data/overview.json";
const STORY_ARTISTS_URL = "/data/artists.json";
const PROOF_URLS = {
  recent_lift_28d: "/data/proofs/recent_lift_28d.json",
  resonance: "/data/proofs/resonance.json",
  underused_winner: "/data/proofs/underused_winner.json",
  catalog_health: "/data/proofs/catalog_health.json",
};

const safeText = (value) => (value === undefined || value === null ? "" : String(value));

const formatShort = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return safeText(value);
  const abs = Math.abs(num);
  if (abs >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toFixed(0);
};

const formatPercent = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return safeText(value);
  const pct = num <= 1 ? num * 100 : num;
  return `${pct.toFixed(2)}%`;
};

const fetchJson = async (url) => {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch ${url} (${resp.status})`);
  return resp.json();
};

const initChipsDemo = async () => {
  const root = document.querySelector("[data-story-layer]");
  if (!root) return;

  const input = document.getElementById("storyArtistInput");
  const addBtn = document.getElementById("storyAddArtist");
  const scanBtn = document.getElementById("storyScanBtn");
  const chipsEl = document.getElementById("storyChips");
  const cardsEl = document.getElementById("storyArtistCards");
  const statusEl = document.getElementById("storyStatus");

  if (!input || !addBtn || !scanBtn || !chipsEl || !cardsEl || !statusEl) return;

  let selected = [];
  let cachedArtists = null;
  let isShowingCards = false;

  const setStatus = (msg) => {
    statusEl.textContent = msg;
  };

  const renderChips = () => {
    chipsEl.innerHTML = "";
    selected.forEach((name, index) => {
      const chip = document.createElement("span");
      chip.className = "chip mono";

      const labelBtn = document.createElement("button");
      labelBtn.type = "button";
      labelBtn.textContent = name;
      labelBtn.setAttribute("aria-label", `Edit ${name}`);
      labelBtn.addEventListener("click", () => {
        input.value = name;
        selected = selected.filter((_, i) => i !== index);
        renderChips();
        setStatus(`Editing: ${name}`);
        input.focus();
      });

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.textContent = "×";
      removeBtn.setAttribute("aria-label", `Remove ${name}`);
      removeBtn.addEventListener("click", () => {
        selected = selected.filter((_, i) => i !== index);
        renderChips();
        setStatus(selected.length ? "Updated artist set (demo)." : "No artists selected (demo)." );
      });

      chip.appendChild(labelBtn);
      chip.appendChild(removeBtn);
      chipsEl.appendChild(chip);
    });

    if (!selected.length) {
      setStatus("Demo mode: enter up to 5 artists.");
    }
  };

  const addChip = () => {
    const raw = input.value.trim();
    if (!raw) return;

    if (selected.length >= 5) {
      setStatus("Demo limit reached: max 5 artists.");
      return;
    }

    const normalized = raw.replace(/\s+/g, " ");
    if (selected.some((a) => a.toLowerCase() === normalized.toLowerCase())) {
      setStatus("Already added (demo).");
      input.value = "";
      return;
    }

    selected = [...selected, normalized];
    input.value = "";
    renderChips();
    setStatus("Saved (demo). Coming soon: live catalog feed.");
  };

  const renderArtistCards = async () => {
    if (!cachedArtists) {
      cachedArtists = await fetchJson(STORY_ARTISTS_URL);
    }

    cardsEl.innerHTML = "";
    const list = Array.isArray(cachedArtists?.artists) ? cachedArtists.artists : [];

    list.slice(0, 5).forEach((artist) => {
      const card = document.createElement("div");
      card.className = "artist-card";

      const img = document.createElement("img");
      img.src = safeText(artist.image_url || "/project-images/project-img-1.jpg");
      img.alt = safeText(artist.display_name || "Artist");

      const meta = document.createElement("div");
      meta.className = "artist-meta";

      const name = document.createElement("p");
      name.className = "mono artist-name";
      name.textContent = safeText(artist.display_name || "Artist");

      const note = document.createElement("p");
      note.className = "artist-note";
      note.textContent = safeText(artist.note || "Loaded in current dataset (demo).") ;

      meta.appendChild(name);
      meta.appendChild(note);
      card.appendChild(img);
      card.appendChild(meta);
      cardsEl.appendChild(card);
    });
  };

  addBtn.addEventListener("click", addChip);
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      addChip();
    }
  });

  scanBtn.addEventListener("click", async () => {
    try {
      if (!isShowingCards) {
        await renderArtistCards();
        cardsEl.hidden = false;
        isShowingCards = true;
        scanBtn.textContent = "Hide loaded artists";
        setStatus("Demo: showing artists loaded in the current dataset.");
      } else {
        cardsEl.hidden = true;
        isShowingCards = false;
        scanBtn.textContent = "Scan artist catalog now";
        setStatus("Demo hidden. Coming soon: live catalog feed.");
      }

      if (window.ScrollTrigger?.refresh) {
        window.ScrollTrigger.refresh();
      }
    } catch {
      setStatus("Could not load demo artists. (Coming soon)");
    }
  });

  renderChips();
};

let activeChart = null;

const destroyActiveChart = () => {
  if (activeChart && typeof activeChart.destroy === "function") {
    activeChart.destroy();
  }
  activeChart = null;
};

const setStoryText = (overview) => {
  if (!overview) return;

  document.querySelectorAll("[data-story]").forEach((el) => {
    const key = el.dataset.story;
    const value = overview?.[key];
    if (value === undefined || value === null) return;
    el.textContent = safeText(value);
  });

  document.querySelectorAll("[data-story-format]").forEach((el) => {
    const key = el.dataset.storyFormat;
    const value = overview?.[key];
    if (value === undefined || value === null) return;

    const format = el.dataset.format;
    if (format === "short") el.textContent = formatShort(value);
    if (format === "percent") el.textContent = formatPercent(value);
  });
};

const renderProof = (signalKey, proof, canvas, notesList) => {
  destroyActiveChart();

  const type = safeText(proof?.chart?.type || "bar");
  const labels = Array.isArray(proof?.chart?.labels) ? proof.chart.labels : [];
  const data = Array.isArray(proof?.chart?.data) ? proof.chart.data : [];
  const dataLabel = safeText(proof?.chart?.label || "");

  const config = {
    type,
    data: {
      labels,
      datasets: [
        {
          label: dataLabel,
          data,
          backgroundColor: "#27c2e6",
          borderColor: "#27c2e6",
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: !!dataLabel, position: "bottom" },
      },
      scales: {
        x: {
          ticks: {
            callback: (v) => (signalKey === "resonance" ? `${v}%` : formatShort(v)),
          },
        },
        y: {
          ticks: {
            callback: (v) => formatShort(v),
          },
        },
      },
    },
  };

  activeChart = new Chart(canvas, config);

  notesList.innerHTML = "";
  const bullets = Array.isArray(proof?.bullets) ? proof.bullets : [];
  bullets.slice(0, 4).forEach((b) => {
    const li = document.createElement("li");
    li.textContent = safeText(b);
    notesList.appendChild(li);
  });
};

const initSignalPopouts = async () => {
  const dialog = document.getElementById("signalDialog");
  const titleEl = document.getElementById("signalDialogTitle");
  const subtitleEl = document.getElementById("signalDialogSubtitle");
  const closeBtn = document.getElementById("signalDialogClose");
  const canvas = document.getElementById("signalDialogCanvas");
  const notesList = document.getElementById("signalDialogNotes");

  if (!dialog || !titleEl || !subtitleEl || !closeBtn || !canvas || !notesList) return;

  const open = async (signalKey) => {
    const url = PROOF_URLS[signalKey];
    if (!url) return;

    let proof;
    try {
      proof = await fetchJson(url);
    } catch {
      proof = null;
    }

    titleEl.textContent = safeText(proof?.title || "Signal proof");
    subtitleEl.textContent = safeText(proof?.subtitle || "One chart + context");

    if (proof) {
      renderProof(signalKey, proof, canvas, notesList);
    }

    if (!dialog.open) {
      dialog.showModal();
    }

    if (window.ScrollTrigger?.refresh) {
      window.ScrollTrigger.refresh();
    }
  };

  const close = () => {
    destroyActiveChart();
    if (dialog.open) dialog.close();
  };

  closeBtn.addEventListener("click", close);
  dialog.addEventListener("click", (event) => {
    const rect = dialog.getBoundingClientRect();
    const inDialog =
      rect.top <= event.clientY &&
      event.clientY <= rect.top + rect.height &&
      rect.left <= event.clientX &&
      event.clientX <= rect.left + rect.width;

    if (!inDialog) close();
  });

  document.querySelectorAll(".home-about-card.story-signal").forEach((card) => {
    const key = card.dataset.signal;
    if (!key) return;

    card.addEventListener("click", () => open(key));
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        open(key);
      }
    });
  });
};

const initStory = async () => {
  try {
    const overview = await fetchJson(STORY_OVERVIEW_URL);
    setStoryText(overview);
  } catch {
    // Demo-mode fallback: keep placeholder HTML values.
  }

  await initChipsDemo();
  await initSignalPopouts();
};

document.addEventListener("DOMContentLoaded", () => {
  initStory();
});
