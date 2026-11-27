/**
 * k6 壓測情境 B：2,000 條即時 WebSocket 訂閱連線壓測
 *
 * 執行方式：
 *   docker run --rm --network flash-ticket_default -v "$PWD/loadtests:/loadtests" \
 *     grafana/k6 run /loadtests/scenario_b_websocket.js
 */

import ws from "k6/ws";
import { check } from "k6";
import { SharedArray } from "k6/data";
import { Counter, Rate, Trend } from "k6/metrics";

const TARGET_VUS = Number(__ENV.TARGET_VUS || 2000);
const HOLD_SECONDS = Number(__ENV.HOLD_SECONDS || 20);
const WS_BASE_URL = __ENV.WS_BASE_URL || "ws://nginx:80";
const DATA_FILE = __ENV.DATA_FILE || "/loadtests/loadtest_env.json";

const envData = new SharedArray("loadtest_env", function () {
  return [JSON.parse(open(DATA_FILE))];
});

const wsConnected = new Counter("ws_connected_total");
const wsWelcomeReceived = new Counter("ws_welcome_received");
const wsPongReceived = new Counter("ws_pong_received");
const wsLiveEventsReceived = new Counter("ws_live_events_received");
const wsConnectSuccessRate = new Rate("ws_connect_success_rate");
const wsHandshakeMs = new Trend("ws_handshake_ms");

export const options = {
  scenarios: {
    ws_subscribers: {
      executor: "ramping-vus",
      startVUs: Math.min(100, TARGET_VUS),
      stages: [
        { duration: "10s", target: TARGET_VUS },
        { duration: `${HOLD_SECONDS}s`, target: TARGET_VUS },
        { duration: "5s", target: 0 },
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    ws_connect_success_rate: ["rate>0.95"],
  },
};

export default function () {
  const cfg = envData[0];
  const eventId = __ENV.EVENT_ID || cfg.event_id;
  const tokens = cfg.tokens;
  const token = tokens[(__VU - 1) % tokens.length];
  const url = `${WS_BASE_URL}/ws/events/${eventId}?token=${encodeURIComponent(token)}`;

  const startTs = Date.now();
  let gotWelcome = false;

  const res = ws.connect(url, {}, function (socket) {
    socket.on("open", function () {
      wsConnected.add(1);
      wsHandshakeMs.add(Date.now() - startTs);
      socket.setInterval(function () {
        socket.send("ping");
      }, 8000);
      socket.setTimeout(function () {
        socket.close();
      }, HOLD_SECONDS * 1000);
    });

    socket.on("message", function (raw) {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === "connected") {
          gotWelcome = true;
          wsWelcomeReceived.add(1);
        } else if (msg.type === "pong") {
          wsPongReceived.add(1);
        } else if (msg.sequence_id) {
          wsLiveEventsReceived.add(1);
        }
      } catch (_) {}
    });
  });

  const ok = Boolean(res && res.status === 101);
  wsConnectSuccessRate.add(ok);

  check(res, {
    "ws upgraded (101)": (r) => Boolean(r && r.status === 101),
    "received connected handshake": () => gotWelcome,
  });
}
