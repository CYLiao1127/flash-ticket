(() => {
  const SEED_CREDS = {
    user: { email: "user@flashticket.com", password: "UserPass123!" },
    admin: { email: "admin@flashticket.com", password: "AdminPass123!" },
  };

  let token = sessionStorage.getItem("ft_token") || "";
  let currentUser = null;
  let currentEventId = null;
  let lastSeq = 0;
  let seatsMap = new Map();
  let zoneMetaMap = new Map();
  let zoneOrder = [];
  let selectedSeatIds = new Set();
  let ws = null;
  let wsPingTimer = null;
  let wsReconnectTimer = null;

  const $ = (id) => document.getElementById(id);

  function log(msg) {
    const box = $("eventLog");
    const ts = new Date().toLocaleTimeString();
    box.textContent = `[${ts}] ${msg}\n` + box.textContent;
  }

  async function api(path, options = {}) {
    const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const res = await fetch(path, { ...options, headers });
    const reqId = res.headers.get("X-Request-ID");
    let body = null;
    try {
      body = await res.json();
    } catch (_) {
      body = null;
    }
    if (!res.ok) {
      const detail = body && body.detail ? JSON.stringify(body.detail) : res.statusText;
      throw new Error(`HTTP ${res.status} (${reqId || "-"}): ${detail}`);
    }
    return { data: body, reqId };
  }

  function updateAuthUI() {
    const loggedIn = Boolean(token && currentUser);
    $("authStatus").textContent = loggedIn
      ? `${currentUser.email} (${currentUser.role})`
      : "尚未登入";
    $("authStatus").className = `badge ${loggedIn ? "ok" : "warn"}`;
    $("btnLogout").disabled = !loggedIn;
    $("btnRefreshOrders").disabled = !loggedIn;
    const hasEvent = loggedIn && Boolean(currentEventId);
    $("btnReloadSnapshot").disabled = !hasEvent;
    $("btnAutoAllocate").disabled = !hasEvent;
    $("zoneFilter").disabled = !hasEvent;
    if ($("bookingWorkspace")) $("bookingWorkspace").hidden = !loggedIn;
    if ($("bookingAuthGate")) $("bookingAuthGate").hidden = loggedIn;
    if ($("eventStatGroup")) $("eventStatGroup").hidden = !hasEvent;
    if ($("seatStepCard")) $("seatStepCard").hidden = !hasEvent;
    if ($("seatStepGate")) $("seatStepGate").hidden = hasEvent;
    if (!hasEvent) {
      $("seqBadge").textContent = "last_seq: -";
      $("statAvailable").textContent = "可選: -";
      $("statHeld").textContent = "鎖位中: -";
      $("statSold").textContent = "已售出: -";
    }
    updateSelectionUI();
  }

  function updateSelectionUI() {
    const count = selectedSeatIds.size;
    $("selectedInfo").textContent = `已選 ${count} / 4 張`;
    $("btnReserveSelected").disabled = !token || !currentEventId || count === 0;
    $("btnClearSelection").disabled = count === 0;
  }

  function renderSeats() {
    const grid = $("seatGrid");
    const filterZone = $("zoneFilter").value;
    let avail = 0, held = 0, sold = 0;
    const grouped = new Map();

    for (const seat of seatsMap.values()) {
      if (seat.status === "available") avail++;
      else if (seat.status === "held") held++;
      else if (seat.status === "sold") sold++;
      if (!filterZone || seat.zone_code === filterZone) {
        if (!grouped.has(seat.zone_code)) grouped.set(seat.zone_code, []);
        grouped.get(seat.zone_code).push(seat);
      }
    }

    $("statAvailable").textContent = `可選: ${avail}`;
    $("statHeld").textContent = `鎖位中: ${held}`;
    $("statSold").textContent = `已售出: ${sold}`;
    $("seqBadge").textContent = `last_seq: ${lastSeq}`;

    if (grouped.size === 0) {
      grid.innerHTML = `<div class="muted">此活動或分區目前無座位資料</div>`;
      return;
    }

    const prevScrollTop = grid.scrollTop;
    grid.innerHTML = "";
    const frag = document.createDocumentFragment();
    const orderedCodes = [
      ...zoneOrder.filter((code) => grouped.has(code)),
      ...Array.from(grouped.keys()).filter((code) => !zoneOrder.includes(code)),
    ];

    for (const zoneCode of orderedCodes) {
      const zoneSeats = (grouped.get(zoneCode) || []).slice().sort((a, b) => {
        const rowCmp = String(a.row_label).localeCompare(String(b.row_label), undefined, { numeric: true });
        if (rowCmp !== 0) return rowCmp;
        return String(a.seat_number).localeCompare(String(b.seat_number), undefined, { numeric: true });
      });
      const paletteIdx = Math.max(0, zoneOrder.indexOf(zoneCode)) % 5;
      const meta = zoneMetaMap.get(zoneCode) || {};
      const zAvail = zoneSeats.filter((s) => s.status === "available").length;

      const section = document.createElement("div");
      section.className = `zone-section zone-palette-${paletteIdx}`;

      const header = document.createElement("div");
      header.className = "zone-section-header";
      const priceText = meta.price ? `NT$ ${Number(meta.price).toLocaleString()}` : "";
      const zoneName = meta.name && meta.name !== zoneCode ? meta.name : `${zoneCode} 區`;
      header.innerHTML = `
        <span class="zone-title">
          <span class="zone-pill">${zoneCode}</span>
          <span>${zoneName}</span>
        </span>
        <span class="zone-meta">
          ${priceText ? `<span>${priceText}</span>` : ""}
          <span>可選 ${zAvail} / ${zoneSeats.length}</span>
        </span>
      `;
      section.appendChild(header);

      const zoneGrid = document.createElement("div");
      zoneGrid.className = "zone-seat-grid";
      for (const seat of zoneSeats) {
        const div = document.createElement("div");
        const isSelected = selectedSeatIds.has(seat.id) && seat.status === "available";
        div.className = `seat ${isSelected ? "selected" : seat.status}`;
        div.textContent = `${seat.row_label}-${seat.seat_number}`;
        div.title = `${zoneCode} 區 第 ${seat.row_label} 排 ${seat.seat_number} 號 (${seat.status})`;
        div.onclick = () => {
          if (seat.status !== "available") return;
          if (selectedSeatIds.has(seat.id)) {
            selectedSeatIds.delete(seat.id);
          } else {
            if (selectedSeatIds.size >= 4) {
              log("單筆最多選取 4 張座位");
              return;
            }
            selectedSeatIds.add(seat.id);
          }
          updateSelectionUI();
          renderSeats();
        };
        zoneGrid.appendChild(div);
      }
      section.appendChild(zoneGrid);
      frag.appendChild(section);
    }
    grid.appendChild(frag);
    grid.scrollTop = prevScrollTop;
  }

  function applySeatDelta(eventType, payload) {
    const newStatus =
      eventType === "seat_held"
        ? "held"
        : eventType === "seat_sold"
        ? "sold"
        : eventType === "seat_released"
        ? "available"
        : null;
    if (!newStatus || !payload) return;

    const list = payload.seats || [];
    const seatIds = payload.seat_ids || list.map((s) => s.seat_id);
    for (const sid of seatIds) {
      const existing = seatsMap.get(sid);
      if (existing) {
        existing.status = newStatus;
        if (newStatus !== "available") selectedSeatIds.delete(sid);
      }
    }
    updateSelectionUI();
    renderSeats();
  }

  async function recoverGapOrResync(expectedAfterSeq) {
    try {
      const { data } = await api(
        `/api/v1/events/${currentEventId}/updates?after_seq=${expectedAfterSeq}&limit=200`
      );
      if (data.resync_required) {
        log(`偵測到大型跳號，重新拉取完整快照 (last_seq=${data.current_last_seq})`);
        await loadSeatSnapshot(currentEventId);
        return;
      }
      for (const ev of data.events) {
        if (ev.sequence_id > lastSeq) {
          applySeatDelta(ev.event_type, ev.payload);
          lastSeq = ev.sequence_id;
        }
      }
      renderSeats();
      log(`斷線補償完成，已同步至 seq=${lastSeq}`);
    } catch (err) {
      log(`補償失敗，改拉完整快照: ${err.message}`);
      await loadSeatSnapshot(currentEventId);
    }
  }

  function closeWS() {
    if (wsPingTimer) clearInterval(wsPingTimer);
    if (wsReconnectTimer) clearTimeout(wsReconnectTimer);
    wsPingTimer = null;
    wsReconnectTimer = null;
    if (ws) {
      ws.onclose = null;
      ws.close();
      ws = null;
    }
    $("wsBadge").textContent = "WS 未連線";
    $("wsBadge").className = "badge warn";
  }

  function connectWS(eventId) {
    closeWS();
    if (!token || !eventId) return;
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${location.host}/ws/events/${eventId}?token=${encodeURIComponent(token)}&last_seq=${lastSeq}`;
    ws = new WebSocket(url);

    ws.onopen = () => {
      $("wsBadge").textContent = "WS 已連線";
      $("wsBadge").className = "badge ok";
      wsPingTimer = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) ws.send("ping");
      }, 25000);
    };

    ws.onmessage = async (evt) => {
      let msg;
      try {
        msg = JSON.parse(evt.data);
      } catch (_) {
        return;
      }
      if (msg.type === "pong") return;
      if (msg.type === "connected") {
        log(`WS 連線成功 (${msg.server_instance}, last_seq=${msg.last_seq})`);
        return;
      }
      if (msg.type === "resync_required") {
        log(`收到伺服器 resync_required (${msg.reason || "gap"})，重拉快照`);
        await loadSeatSnapshot(eventId);
        return;
      }
      if (typeof msg.sequence_id === "number") {
        if (msg.sequence_id <= lastSeq) return;
        if (msg.sequence_id > lastSeq + 1) {
          log(`偵測跳號 expected=${lastSeq + 1} got=${msg.sequence_id}，啟動補償`);
          await recoverGapOrResync(lastSeq);
          return;
        }
        lastSeq = msg.sequence_id;
        applySeatDelta(msg.event_type, msg.payload);
        log(`即時更新 seq=${msg.sequence_id} (${msg.event_type})`);
      }
    };

    ws.onclose = (evt) => {
      $("wsBadge").textContent = `WS 斷線 (${evt.code})`;
      $("wsBadge").className = "badge err";
      if (wsPingTimer) clearInterval(wsPingTimer);
      if (token && currentEventId === eventId) {
        wsReconnectTimer = setTimeout(() => connectWS(eventId), 2000);
      }
    };
  }

  async function loadEvents() {
    const { data } = await api("/api/v1/events?limit=50");
    const sel = $("eventSelect");
    const events = data.items || [];
    sel.innerHTML = `<option value="">-- 請選擇活動 --</option>`;
    for (const ev of events) {
      const opt = document.createElement("option");
      opt.value = ev.id;
      opt.textContent = `#${ev.id} ${ev.title} (${ev.status})`;
      sel.appendChild(opt);
    }
    // 採漸進式流程：登入後由使用者主動挑選活動，再展開分區座位圖與建立 WS 連線
    sel.value = "";
    currentEventId = null;
    closeWS();
    $("zoneFilter").innerHTML = `<option value="">請先選擇活動...</option>`;
    updateAuthUI();
    log(`已載入 ${events.length} 場活動，請先選擇欲購票的場次`);
  }

  async function loadSeatSnapshot(eventId) {
    const { data } = await api(`/api/v1/events/${eventId}/seats?limit=500`);
    lastSeq = data.last_seq || 0;
    seatsMap.clear();
    selectedSeatIds.clear();
    for (const s of data.seats || []) seatsMap.set(s.id, s);

    const summaries = data.zones || data.zone_summaries || [];
    zoneMetaMap.clear();
    zoneOrder = summaries.map((z) => z.zone_code);
    for (const z of summaries) zoneMetaMap.set(z.zone_code, z);

    const zones = zoneOrder;
    const zFilter = $("zoneFilter");
    const zAlloc = $("allocZone");
    const prevFilter = zFilter.value;
    zFilter.innerHTML = `<option value="">全部區域</option>`;
    zAlloc.innerHTML = "";
    for (const z of summaries) {
      const label = z.name && z.name !== z.zone_code ? `${z.zone_code} — ${z.name}` : `${z.zone_code} 區`;
      zFilter.innerHTML += `<option value="${z.zone_code}">${label}</option>`;
      zAlloc.innerHTML += `<option value="${z.zone_code}">${label}</option>`;
    }
    if (zones.includes(prevFilter)) zFilter.value = prevFilter;
    updateSelectionUI();
    renderSeats();
    log(`已載入活動 #${eventId} 快照 (共 ${seatsMap.size} 座位, last_seq=${lastSeq})`);
  }

  async function selectEvent(eventId) {
    currentEventId = Number(eventId) || null;
    if (!currentEventId) {
      closeWS();
      seatsMap.clear();
      zoneMetaMap.clear();
      zoneOrder = [];
      selectedSeatIds.clear();
      $("zoneFilter").innerHTML = `<option value="">請先選擇活動...</option>`;
      updateAuthUI();
      return;
    }
    updateAuthUI();
    await loadSeatSnapshot(currentEventId);
    connectWS(currentEventId);
  }

  async function loadOrders() {
    const { data } = await api("/api/v1/orders?limit=20");
    const box = $("ordersList");
    const items = data.items || [];
    if (items.length === 0) {
      box.innerHTML = `<div class="muted">尚無訂單</div>`;
      return;
    }
    box.innerHTML = items
      .map(
        (o) => `
      <div class="order-item">
        <div class="order-heading"><strong>訂單 #${o.id}</strong><span class="badge ${o.status === "paid" ? "ok" : o.status === "pending" ? "warn" : "info"}">${o.status}</span></div>
        <div class="order-total">總額 NT$ ${o.total_amount} <span class="muted">（票價 ${o.total_ticket_amount} + 手續費 ${o.service_fee}）</span></div>
        ${
          o.status === "pending"
            ? `<div class="row" style="margin-top:6px;">
                 <button class="success" onclick="window.__payOrder(${o.id})">模擬付款</button>
                 <button class="danger" onclick="window.__cancelOrder(${o.id})">取消訂單</button>
               </div>`
            : ""
        }
      </div>`
      )
      .join("");
  }

  window.__payOrder = async (orderId) => {
    try {
      const { data, reqId } = await api("/api/v1/payments/mock/pay", {
        method: "POST",
        body: JSON.stringify({ order_id: orderId, outcome: "succeeded" }),
      });
      const st = data && data.webhook_result ? data.webhook_result.order_status : "paid";
      log(`訂單 #${orderId} 付款成功 (${st}, X-Request-ID=${reqId})`);
      await loadOrders();
    } catch (err) {
      log(`付款失敗: ${err.message}`);
    }
  };

  window.__cancelOrder = async (orderId) => {
    try {
      const { reqId } = await api(`/api/v1/orders/${orderId}/cancel`, { method: "POST" });
      log(`訂單 #${orderId} 已取消並釋放座位 (X-Request-ID=${reqId})`);
      await loadOrders();
    } catch (err) {
      log(`取消失敗: ${err.message}`);
    }
  };

  async function login() {
    const errEl = $("loginError");
    if (errEl) {
      errEl.hidden = true;
      errEl.textContent = "";
    }
    try {
      const email = $("loginEmail").value.trim();
      const password = $("loginPassword").value;
      const { data } = await api("/api/v1/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      token = data.access_token;
      currentUser = data.user;
      sessionStorage.setItem("ft_token", token);
      updateAuthUI();
      log(`登入成功: ${currentUser.email}`);
      await Promise.all([loadEvents(), loadOrders()]);
    } catch (err) {
      if (errEl) {
        errEl.textContent = `登入失敗：${err.message}`;
        errEl.hidden = false;
      }
      log(`登入失敗: ${err.message}`);
    }
  }

  async function logout() {
    try {
      await api("/api/v1/auth/logout", { method: "POST" });
    } catch (_) {}
    closeWS();
    token = "";
    currentUser = null;
    currentEventId = null;
    lastSeq = 0;
    seatsMap.clear();
    zoneMetaMap.clear();
    zoneOrder = [];
    selectedSeatIds.clear();
    $("seatGrid").innerHTML = `<div class="muted">請選擇活動載入座位快照...</div>`;
    $("ordersList").innerHTML = `<div class="muted">登入後顯示您的訂單...</div>`;
    sessionStorage.removeItem("ft_token");
    updateAuthUI();
    log("已登出並即時撤銷所有既有憑證");
  }

  $("btnLogin").onclick = login;
  $("btnLogout").onclick = logout;
  $("btnRefreshOrders").onclick = loadOrders;
  $("btnReloadSnapshot").onclick = () => currentEventId && loadSeatSnapshot(currentEventId);
  $("eventSelect").onchange = (e) => selectEvent(e.target.value);
  $("zoneFilter").onchange = renderSeats;
  $("btnClearSelection").onclick = () => {
    selectedSeatIds.clear();
    updateSelectionUI();
    renderSeats();
  };

  $("btnReserveSelected").onclick = async () => {
    try {
      const idemKey = `ui-res-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const { data, reqId } = await api(`/api/v1/events/${currentEventId}/tickets/reserve`, {
        method: "POST",
        headers: { "Idempotency-Key": idemKey },
        body: JSON.stringify({ seat_ids: Array.from(selectedSeatIds) }),
      });
      selectedSeatIds.clear();
      updateSelectionUI();
      const orderId = data.id ?? data.order_id;
      log(`指定選位成功！訂單 #${orderId}，總額 NT$${data.total_amount} (X-Request-ID=${reqId})`);
      await loadOrders();
    } catch (err) {
      log(`指定選位失敗: ${err.message}`);
      await loadSeatSnapshot(currentEventId);
    }
  };

  $("btnAutoAllocate").onclick = async () => {
    try {
      const zoneCode = $("allocZone").value;
      const quantity = Number($("allocQty").value);
      const idemKey = `ui-alloc-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const { data, reqId } = await api(`/api/v1/events/${currentEventId}/tickets/allocate`, {
        method: "POST",
        headers: { "Idempotency-Key": idemKey },
        body: JSON.stringify({ zone_code: zoneCode, quantity }),
      });
      const orderId = data.id ?? data.order_id;
      log(`自動配位成功！訂單 #${orderId} (${zoneCode} 區 ${quantity} 張, X-Request-ID=${reqId})`);
      await loadOrders();
    } catch (err) {
      log(`自動配位失敗: ${err.message}`);
    }
  };

  document.querySelectorAll("[data-fill]").forEach((btn) => {
    btn.onclick = () => {
      const cred = SEED_CREDS[btn.dataset.fill];
      $("loginEmail").value = cred.email;
      $("loginPassword").value = cred.password;
    };
  });

  // 嘗試沿用已儲存 Token
  if (token) {
    api("/api/v1/auth/me")
      .then(async ({ data }) => {
        currentUser = data;
        updateAuthUI();
        await Promise.all([loadEvents(), loadOrders()]);
      })
      .catch(() => {
        token = "";
        sessionStorage.removeItem("ft_token");
        updateAuthUI();
      });
  } else {
    updateAuthUI();
  }
})();
