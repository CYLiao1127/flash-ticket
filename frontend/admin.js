(() => {
  const SEED_ADMIN = {
    admin: { email: "admin@flashticket.com", password: "AdminPass123!" },
    viewer: { email: "viewer@flashticket.com", password: "ViewerPass123!" },
  };

  let token = sessionStorage.getItem("ft_admin_token") || "";
  let currentUser = null;
  let pollTimer = null;
  let sseAbort = null;
  let sseReconnectTimer = null;
  let alertsBuffer = [];

  const $ = (id) => document.getElementById(id);

  async function api(path, options = {}) {
    const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const res = await fetch(path, { ...options, headers });
    let body = null;
    try {
      body = await res.json();
    } catch (_) {}
    if (!res.ok) {
      throw new Error(body && body.detail ? JSON.stringify(body.detail) : res.statusText);
    }
    return body;
  }

  function _fmtTime(ts) {
    if (!ts) return "";
    const d = new Date(ts);
    return d.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function _svgEl(tag, attrs = {}, textContent = null) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, String(v));
    if (textContent !== null) el.textContent = textContent;
    return el;
  }

  function renderPolylineChart(svgId, seriesList, options = {}) {
    const { timestamps = [], yUnit = "", xLabel = "時間 (HH:MM:SS)" } = options;
    const svg = $(svgId);
    const w = svg.clientWidth || 640;
    const h = svg.clientHeight || 170;
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    svg.innerHTML = "";

    const allValues = seriesList.flatMap((s) => s.values);
    if (allValues.length === 0) {
      svg.appendChild(_svgEl("text", { x: 16, y: 30, fill: "#7a7f76", "font-size": 12 }, "尚無每秒指標資料"));
      return;
    }

    const maxVal = Math.max(1, ...allValues);
    const padLeft = 56;
    const padRight = 14;
    const padTop = 22;
    const padBottom = 28;
    const usableW = Math.max(20, w - padLeft - padRight);
    const usableH = Math.max(20, h - padTop - padBottom);

    // Y 軸單位標示
    if (yUnit) {
      svg.appendChild(
        _svgEl("text", { x: 4, y: 13, fill: "#6d726a", "font-size": 10, "font-weight": 700 }, `單位: ${yUnit}`)
      );
    }

    // 水平參考格線與 Y 軸刻度 (0%, 50%, 100%)
    for (const ratio of [0, 0.5, 1]) {
      const y = h - padBottom - ratio * usableH;
      const tickVal = maxVal * ratio;
      const tickText = tickVal >= 100 ? Math.round(tickVal).toString() : tickVal.toFixed(ratio === 0 ? 0 : 1);
      svg.appendChild(
        _svgEl("line", {
          x1: padLeft,
          y1: y.toFixed(1),
          x2: w - padRight,
          y2: y.toFixed(1),
          stroke: ratio === 0 ? "#cfc8bc" : "#e6e0d5",
          "stroke-width": 1,
          "stroke-dasharray": ratio === 0 ? "none" : "3,3",
        })
      );
      svg.appendChild(
        _svgEl(
          "text",
          { x: padLeft - 6, y: (y + 3.5).toFixed(1), fill: "#6d726a", "font-size": 10, "text-anchor": "end" },
          tickText
        )
      );
    }

    // Y 軸主線
    svg.appendChild(
      _svgEl("line", {
        x1: padLeft,
        y1: padTop,
        x2: padLeft,
        y2: h - padBottom,
        stroke: "#cfc8bc",
        "stroke-width": 1,
      })
    );

    // X 軸時間刻度（起點 / 中點 / 終點）與軸名
    if (timestamps.length > 0) {
      const idxs =
        timestamps.length === 1
          ? [0]
          : timestamps.length === 2
          ? [0, 1]
          : [0, Math.floor((timestamps.length - 1) / 2), timestamps.length - 1];
      const step = timestamps.length > 1 ? usableW / (timestamps.length - 1) : 0;
      idxs.forEach((idx, pos) => {
        const x = padLeft + idx * step;
        const anchor = pos === 0 ? "start" : pos === idxs.length - 1 ? "end" : "middle";
        svg.appendChild(
          _svgEl(
            "text",
            { x: x.toFixed(1), y: h - 11, fill: "#6d726a", "font-size": 10, "text-anchor": anchor },
            _fmtTime(timestamps[idx])
          )
        );
      });
    }
    svg.appendChild(
      _svgEl(
        "text",
        { x: (padLeft + usableW / 2).toFixed(1), y: h - 1, fill: "#8c9188", "font-size": 9.5, "text-anchor": "middle" },
        xLabel
      )
    );

    // 右上角圖例
    let legendX = w - padRight;
    for (let i = seriesList.length - 1; i >= 0; i--) {
      const s = seriesList[i];
      if (!s.label) continue;
      const lbl = _svgEl(
        "text",
        { x: legendX, y: 13, fill: s.color, "font-size": 10, "font-weight": 700, "text-anchor": "end" },
        `● ${s.label}`
      );
      svg.appendChild(lbl);
      legendX -= s.label.length * 7 + 26;
    }

    // 繪製折線
    for (const s of seriesList) {
      if (s.values.length === 0) continue;
      const step = s.values.length > 1 ? usableW / (s.values.length - 1) : 0;
      const pts = s.values
        .map((v, i) => {
          const x = padLeft + i * step;
          const y = h - padBottom - (v / maxVal) * usableH;
          return `${x.toFixed(1)},${y.toFixed(1)}`;
        })
        .join(" ");

      svg.appendChild(
        _svgEl("polyline", {
          fill: "none",
          stroke: s.color,
          "stroke-width": 2.2,
          "stroke-linejoin": "round",
          points: pts,
        })
      );
    }
  }

  function renderAlerts() {
    $("kpiAlerts").textContent = String(alertsBuffer.length);
    const feed = $("alertsFeed");
    if (alertsBuffer.length === 0) {
      feed.innerHTML = `<div class="muted">目前系統正常，無異常告警</div>`;
      return;
    }
    feed.innerHTML = alertsBuffer
      .slice(0, 30)
      .map(
        (a) => `
      <div class="alert-item ${a.severity === "critical" ? "critical" : ""}">
        <div><strong>[${a.severity.toUpperCase()}] ${a.title}</strong></div>
        <div>${a.message}</div>
        <div class="muted">規則: ${a.rule} | 時間: ${new Date(a.triggered_at).toLocaleString()}</div>
      </div>`
      )
      .join("");
  }

  async function pollMetrics() {
    if (!token) return;
    try {
      const data = await api("/api/v1/admin/monitoring/metrics?limit=60");
      $("kpiRps").textContent = Number(data.avg_rps || 0).toFixed(2);
      $("kpiErrRate").textContent = `${(Number(data.error_rate || 0) * 100).toFixed(2)}%`;
      $("kpiP99").textContent = `${Number(data.max_p99_latency_ms || 0).toFixed(1)}`;
      $("kpiConns").textContent = String(data.current_active_connections ?? 0);
      $("lastUpdated").textContent = `更新時間: ${new Date().toLocaleTimeString()}`;

      const rows = data.series || [];
      const timestamps = rows.map((r) => r.timestamp);
      renderPolylineChart(
        "chartRps",
        [
          { label: "RPS", color: "#477f9e", values: rows.map((r) => Number(r.total_requests)) },
          { label: "5xx", color: "#cf4d55", values: rows.map((r) => Number(r.error_requests)) },
        ],
        { timestamps, yUnit: "req/s", xLabel: "時間 (HH:MM:SS)" }
      );
      renderPolylineChart(
        "chartLatency",
        [
          { label: "p50", color: "#27866a", values: rows.map((r) => Number(r.p50_latency_ms)) },
          { label: "p95", color: "#b7791f", values: rows.map((r) => Number(r.p95_latency_ms)) },
          { label: "p99", color: "#cf4d55", values: rows.map((r) => Number(r.p99_latency_ms)) },
        ],
        { timestamps, yUnit: "ms", xLabel: "時間 (HH:MM:SS)" }
      );
    } catch (err) {
      $("lastUpdated").textContent = `讀取失敗: ${err.message}`;
    }
  }

  async function startAlertSSE() {
    stopAlertSSE();
    if (!token) return;
    sseAbort = new AbortController();
    $("sseBadge").textContent = "告警串流連線中...";
    $("sseBadge").className = "badge info";

    try {
      const res = await fetch("/api/v1/admin/monitoring/alerts/stream", {
        headers: { Authorization: `Bearer ${token}` },
        signal: sseAbort.signal,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      $("sseBadge").textContent = "告警串流已連線 (SSE)";
      $("sseBadge").className = "badge ok";

      const reader = res.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buf = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const frame = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          let evName = "message";
          let dataStr = "";
          for (const line of frame.split("\n")) {
            if (line.startsWith("event:")) evName = line.slice(6).trim();
            else if (line.startsWith("data:")) dataStr += line.slice(5).trim();
          }
          if (!dataStr) continue;
          const payload = JSON.parse(dataStr);
          if (evName === "connected") {
            alertsBuffer = payload.recent_alerts || [];
            renderAlerts();
          } else if (evName === "alert") {
            alertsBuffer.unshift(payload);
            renderAlerts();
          }
        }
      }
      if (token && sseAbort && !sseAbort.signal.aborted) {
        $("sseBadge").textContent = "告警串流重連中...";
        $("sseBadge").className = "badge warn";
        sseReconnectTimer = setTimeout(startAlertSSE, 3000);
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        $("sseBadge").textContent = "告警串流中斷（自動重連中）";
        $("sseBadge").className = "badge err";
        if (token) {
          sseReconnectTimer = setTimeout(startAlertSSE, 3000);
        }
      }
    }
  }

  function stopAlertSSE() {
    if (sseReconnectTimer) {
      clearTimeout(sseReconnectTimer);
      sseReconnectTimer = null;
    }
    if (sseAbort) {
      sseAbort.abort();
      sseAbort = null;
    }
    $("sseBadge").textContent = "告警串流未連線";
    $("sseBadge").className = "badge warn";
  }

  async function loadUsers() {
    const tbody = $("usersTableBody");
    if (!currentUser || currentUser.role !== "admin") {
      tbody.innerHTML = `<tr><td colspan="6" class="muted">目前角色 (${currentUser ? currentUser.role : "未登入"}) 僅限唯讀檢視</td></tr>`;
      return;
    }
    const users = await api("/api/v1/admin/users");
    tbody.innerHTML = users
      .map(
        (u) => `
      <tr>
        <td>${u.id}</td>
        <td>${u.email}</td>
        <td><span class="badge info">${u.role}</span></td>
        <td><span class="badge ${u.is_active ? "ok" : "err"}">${u.is_active ? "啟用" : "停用"}</span></td>
        <td>${u.token_version}</td>
        <td>
          ${
            u.id === currentUser.id
              ? `<span class="muted">當前帳號</span>`
              : `<div class="row" style="gap:6px;">
                   <select aria-label="修改角色" onchange="window.__changeUserRole(${u.id}, this.value)">
                     <option value="admin" ${u.role === "admin" ? "selected" : ""}>admin</option>
                     <option value="user" ${u.role === "user" ? "selected" : ""}>user</option>
                     <option value="viewer" ${u.role === "viewer" ? "selected" : ""}>viewer</option>
                   </select>
                   <button class="secondary" onclick="window.__toggleUser(${u.id}, ${!u.is_active})">${u.is_active ? "停用" : "啟用"}</button>
                 </div>`
          }
        </td>
      </tr>`
      )
      .join("");
  }

  window.__changeUserRole = async (userId, nextRole) => {
    try {
      await api(`/api/v1/admin/users/${userId}/role`, {
        method: "PATCH",
        body: JSON.stringify({ role: nextRole }),
      });
      await loadUsers();
    } catch (err) {
      alert(`修改角色失敗: ${err.message}`);
      await loadUsers();
    }
  };

  window.__toggleUser = async (userId, nextActive) => {
    try {
      await api(`/api/v1/admin/users/${userId}/status`, {
        method: "PATCH",
        body: JSON.stringify({ is_active: nextActive }),
      });
      await loadUsers();
    } catch (err) {
      alert(`操作失敗: ${err.message}`);
    }
  };

  function updateUI() {
    const loggedIn = Boolean(token && currentUser);
    const isAdmin = loggedIn && currentUser.role === "admin";
    $("adminAuthStatus").textContent = loggedIn
      ? `${currentUser.email} (${currentUser.role})`
      : "尚未登入";
    $("adminAuthStatus").className = `badge ${loggedIn ? "ok" : "warn"}`;
    $("btnAdminLogout").disabled = !loggedIn;
    $("btnTrace").disabled = !loggedIn;
    $("btnLoadLogs").disabled = !loggedIn;
    $("btnEvalAlerts").disabled = !isAdmin;
    $("btnRefreshUsers").disabled = !isAdmin;
    if ($("adminWorkspace")) $("adminWorkspace").hidden = !loggedIn;
    if ($("adminAuthGate")) $("adminAuthGate").hidden = loggedIn;
  }

  async function startDashboard() {
    updateUI();
    await pollMetrics();
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(pollMetrics, 2000);
    startAlertSSE();
    await loadUsers();
  }

  $("btnAdminLogin").onclick = async () => {
    try {
      const data = await api("/api/v1/auth/login", {
        method: "POST",
        body: JSON.stringify({
          email: $("adminEmail").value.trim(),
          password: $("adminPassword").value,
        }),
      });
      token = data.access_token;
      currentUser = data.user;
      sessionStorage.setItem("ft_admin_token", token);
      await startDashboard();
    } catch (err) {
      alert(`登入失敗: ${err.message}`);
    }
  };

  $("btnAdminLogout").onclick = async () => {
    try {
      await api("/api/v1/auth/logout", { method: "POST" });
    } catch (_) {}
    stopAlertSSE();
    if (pollTimer) clearInterval(pollTimer);
    token = "";
    currentUser = null;
    sessionStorage.removeItem("ft_admin_token");
    updateUI();
  };

  $("btnEvalAlerts").onclick = async () => {
    try {
      const res = await api("/api/v1/admin/monitoring/alerts/evaluate", { method: "POST" });
      if (res.dispatched_count === 0) {
        alert("目前所有指標正常（或仍在冷卻期內），未觸發新告警。");
      }
    } catch (err) {
      alert(`評估失敗: ${err.message}`);
    }
  };

  $("btnTrace").onclick = async () => {
    const rid = $("traceInput").value.trim();
    if (!rid) return;
    try {
      const data = await api(`/api/v1/admin/monitoring/trace/${encodeURIComponent(rid)}`);
      $("traceResult").textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      $("traceResult").textContent = `查詢失敗: ${err.message}`;
    }
  };

  $("btnLoadLogs").onclick = async () => {
    try {
      const data = await api("/api/v1/admin/monitoring/access-logs?limit=15");
      $("traceResult").textContent = (data.items || [])
        .map(
          (l) =>
            `[${l.created_at}] ${l.method} ${l.path} status=${l.status_code} ${l.latency_ms}ms req_id=${l.request_id}`
        )
        .join("\n") || "尚無存取日誌";
    } catch (err) {
      $("traceResult").textContent = `載入失敗: ${err.message}`;
    }
  };

  $("btnRefreshUsers").onclick = loadUsers;

  document.querySelectorAll("[data-role]").forEach((btn) => {
    btn.onclick = () => {
      const cred = SEED_ADMIN[btn.dataset.role];
      $("adminEmail").value = cred.email;
      $("adminPassword").value = cred.password;
    };
  });

  if (token) {
    api("/api/v1/auth/me")
      .then(async (user) => {
        currentUser = user;
        await startDashboard();
      })
      .catch(() => {
        token = "";
        sessionStorage.removeItem("ft_admin_token");
        updateUI();
      });
  } else {
    updateUI();
  }
})();
