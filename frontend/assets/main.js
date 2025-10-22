const form = document.getElementById("query-form");
const resultSection = document.getElementById("result");
const statusEl = document.getElementById("status");
const quarterFields = document.querySelectorAll(".quarter-only");

form.addEventListener("change", (event) => {
  if (event.target.name === "period_type") {
    const showQuarter = event.target.value === "quarter";
    quarterFields.forEach((node) => node.classList.toggle("hidden", !showQuarter));
  }
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  statusEl.textContent = "正在拉取数据并生成分析，请稍候...";
  resultSection.classList.add("hidden");

  const formData = Object.fromEntries(new FormData(form).entries());
  const payload = buildPayload(formData);
  if (!payload) {
    statusEl.textContent = "股票代码数量需在 1-10 之间";
    return;
  }

  try {
    const response = await fetch("/api/financials", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || "查询失败");
    }

    const json = await response.json();
    renderResult(json);
    statusEl.textContent = "查询完成";
    resultSection.classList.remove("hidden");
  } catch (error) {
    statusEl.textContent = error.message;
  }
});

function buildPayload(formData) {
  const symbols = (formData.symbols || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (symbols.length === 0 || symbols.length > 10) {
    return null;
  }

  const payload = {
    tushare_token: formData.tushare_token,
    symbols,
    period_type: formData.period_type,
    start_year: Number(formData.start_year),
    end_year: Number(formData.end_year),
    filename: formData.filename,
  };

  if (formData.period_type === "quarter") {
    payload.start_quarter = Number(formData.start_quarter);
    payload.end_quarter = Number(formData.end_quarter);
  }

  return payload;
}

function renderResult(json) {
  const summaryEl = document.getElementById("summary");
  const headRow = document.getElementById("table-head");
  const body = document.getElementById("table-body");
  const downloadLink = document.getElementById("download-link");

  summaryEl.textContent = json.summary;

  headRow.innerHTML = "";
  body.innerHTML = "";

  json.columns.forEach((column) => {
    const th = document.createElement("th");
    th.className = "px-4 py-2 text-left";
    th.textContent = column;
    headRow.appendChild(th);
  });

  json.table.forEach((row) => {
    const tr = document.createElement("tr");
    tr.className = "border-b border-slate-800 hover:bg-slate-800";

    json.columns.forEach((column) => {
      const td = document.createElement("td");
      td.className = "px-4 py-2";
      td.textContent = row[column] ?? "";
      tr.appendChild(td);
    });

    body.appendChild(tr);
  });

  const fileNameInput = form.querySelector("input[name='filename']");
  const downloadName = (fileNameInput?.value || "AlysonG-report") + ".xlsx";
  downloadLink.href = `/api/download/${json.download_token}`;
  downloadLink.download = downloadName;
}
