/**
 * k6 壓測情境 A：高併發搶票壓測（預設 3,000 VU / 10,000 座位 / 持續 60 秒）
 *
 * 執行方式：
 *   docker run --rm --network flash-ticket_default -v "$PWD/loadtests:/loadtests" \
 *     grafana/k6 run /loadtests/scenario_a_booking.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { SharedArray } from "k6/data";
import { Counter, Rate, Trend } from "k6/metrics";

const TARGET_VUS = Number(__ENV.TARGET_VUS || 3000);
const DURATION = __ENV.DURATION || "60s";
const BASE_URL = __ENV.BASE_URL || "http://nginx:80";
const DATA_FILE = __ENV.DATA_FILE || "/loadtests/loadtest_env.json";
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;

const envData = new SharedArray("loadtest_env", function () {
  return [JSON.parse(open(DATA_FILE))];
});

const bookingSuccess = new Counter("booking_success");
const bookingExpected409 = new Counter("booking_expected_409");
const bookingRateLimited429 = new Counter("booking_rate_limited_429");
const bookingServer5xx = new Rate("booking_server_5xx");
const bookingLatency = new Trend("booking_latency_ms");

export const options = {
  scenarios: {
    flash_sale_rush: {
      executor: "ramping-vus",
      startVUs: Math.min(100, TARGET_VUS),
      stages: [
        { duration: "10s", target: TARGET_VUS },
        { duration: DURATION, target: TARGET_VUS },
        { duration: "5s", target: 0 },
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    booking_server_5xx: ["rate<0.05"],
  },
};

export default function () {
  const cfg = envData[0];
  const eventId = __ENV.EVENT_ID || cfg.event_id;
  const tokens = cfg.tokens;
  const zones = cfg.zones;

  const token = tokens[(__VU - 1) % tokens.length];
  const zone = zones[(__VU + __ITER) % zones.length];
  const qty = (__ITER % 2) + 1;
  const idemKey = `k6-a-${RUN_ID}-e${eventId}-u${__VU}-i${__ITER}`;
  const reqId = `k6-a-${__VU}-${__ITER}-${Date.now()}`;

  const res = http.post(
    `${BASE_URL}/api/v1/events/${eventId}/tickets/allocate`,
    JSON.stringify({ zone_code: zone, quantity: qty }),
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        "Idempotency-Key": idemKey,
        "X-Request-ID": reqId,
      },
      timeout: "15s",
    }
  );

  bookingLatency.add(res.timings.duration);
  const is5xx = res.status >= 500 || res.status === 0;
  bookingServer5xx.add(is5xx);

  if (res.status === 201) {
    bookingSuccess.add(1);
  } else if (res.status === 409) {
    bookingExpected409.add(1);
  } else if (res.status === 429) {
    bookingRateLimited429.add(1);
  }

  check(res, {
    "status is 201, 409, or 429 load-shed": (r) =>
      r.status === 201 || r.status === 409 || r.status === 429,
    "no 5xx error": (r) => r.status > 0 && r.status < 500,
  });

  // 貼近真實搶票者的思考／重試間隔 (3~5 秒)
  sleep(3 + Math.random() * 2);
}
