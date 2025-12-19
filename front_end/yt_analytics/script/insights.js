import Chart from "chart.js/auto";

const INSIGHTS_URL = "/data/artist_insights.json";
const PALETTE = ["#ff2d2d", "#ffb347", "#27c2e6", "#82d173", "#f96a6a", "#f3c969"];

const formatNumber = (value) => value.toLocaleString("en-US");

const formatShortNumber = (value) => {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
};

const formatCurrency = (value) => `$${formatShortNumber(value)}`;

const formatPercent = (value) => `${value.toFixed(2)}%`;

const applyInsightText = (data) => {
  const summary = data.summary;
  const artists = data.artists;
  const topArtists = data.top_artists;
  const quality = data.data_quality;

  const byViews = [...artists].sort((a, b) => b.total_views - a.total_views);
  const byEngagement = [...artists].sort(
    (a, b) => b.avg_engagement_rate - a.avg_engagement_rate
  );
  const byEfficiency = [...artists].sort(
    (a, b) => b.revenue_per_video - a.revenue_per_video
  );
  const byMomentum = [...artists].sort(
    (a, b) => b.avg_views_per_day - a.avg_views_per_day
  );

  const insightMap = {
    artist_count: summary.artist_count,
    top_artist_count: topArtists.length,
    total_views: summary.total_views,
    total_videos: summary.total_videos,
    total_est_revenue_usd: summary.total_est_revenue_usd,
    avg_engagement_rate: summary.avg_engagement_rate,
    isrc_null_rate: quality.isrc_null_rate,
    top_reach_name: byViews[0]?.display_name,
    top_reach_value: byViews[0]?.total_views,
    top_resonance_name: byEngagement[0]?.display_name,
    top_resonance_value: byEngagement[0]?.avg_engagement_rate,
    top_efficiency_name: byEfficiency[0]?.display_name,
    top_efficiency_value: byEfficiency[0]?.revenue_per_video,
    momentum_leader_name: byMomentum[0]?.display_name,
    momentum_leader_value: byMomentum[0]?.avg_views_per_day,
  };

  document.querySelectorAll("[data-insight]").forEach((element) => {
    const key = element.dataset.insight;
    const format = element.dataset.format;
    const value = insightMap[key];

    if (value === undefined || value === null) return;
    if (typeof value === "string") {
      element.textContent = value;
      return;
    }

    switch (format) {
      case "short":
        element.textContent = formatShortNumber(value);
        break;
      case "currency":
        element.textContent = formatCurrency(value);
        break;
      case "percent":
        element.textContent = formatPercent(value);
        break;
      case "number":
      default:
        element.textContent = formatNumber(value);
        break;
    }
  });
};

const renderReachResonanceChart = (ctx, artists) => {
  const datasets = artists.map((artist, index) => ({
    label: artist.display_name,
    data: [
      {
        x: artist.total_views,
        y: artist.avg_engagement_rate,
        r: Math.max(6, Math.sqrt(artist.total_videos)),
      },
    ],
    backgroundColor: `${PALETTE[index % PALETTE.length]}99`,
    borderColor: PALETTE[index % PALETTE.length],
    borderWidth: 2,
  }));

  new Chart(ctx, {
    type: "bubble",
    data: { datasets },
    options: {
      responsive: true,
      plugins: {
        legend: { position: "bottom" },
      },
      scales: {
        x: {
          title: { display: true, text: "Total Views" },
          ticks: {
            callback: (value) => formatShortNumber(value),
          },
        },
        y: {
          title: { display: true, text: "Engagement Rate (%)" },
          ticks: {
            callback: (value) => `${value}%`,
          },
        },
      },
    },
  });
};

const renderEfficiencyChart = (ctx, artists) => {
  const labels = artists.map((artist) => artist.display_name);
  const values = artists.map((artist) => artist.revenue_per_video);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Est. revenue per video",
          data: values,
          backgroundColor: PALETTE[0],
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
      },
      scales: {
        y: {
          ticks: {
            callback: (value) => formatCurrency(value),
          },
        },
      },
    },
  });
};

const renderVideoTypeChart = (ctx, videoTypes) => {
  const sorted = [...videoTypes].sort((a, b) => b.video_count - a.video_count);
  const labels = sorted.map((item) => item.video_type);
  const values = sorted.map((item) => item.video_count);

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: PALETTE,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { position: "bottom" },
      },
    },
  });
};

const initInsights = async () => {
  const response = await fetch(INSIGHTS_URL);
  if (!response.ok) return;
  const data = await response.json();

  applyInsightText(data);

  const reachCtx = document.getElementById("chart-reach-resonance");
  const efficiencyCtx = document.getElementById("chart-efficiency");
  const videoTypeCtx = document.getElementById("chart-video-types");

  if (reachCtx) {
    renderReachResonanceChart(reachCtx, data.artists);
  }

  if (efficiencyCtx) {
    const topByEfficiency = [...data.artists]
      .sort((a, b) => b.revenue_per_video - a.revenue_per_video)
      .slice(0, 5);
    renderEfficiencyChart(efficiencyCtx, topByEfficiency);
  }

  if (videoTypeCtx) {
    renderVideoTypeChart(videoTypeCtx, data.video_types);
  }
};

document.addEventListener("DOMContentLoaded", () => {
  initInsights();
});
