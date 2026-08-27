import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const appURL = requiredEnv('CAPACITY_APP_URL');
const runID = requiredEnv('CAPACITY_RUN_ID');
const stageID = requiredEnv('CAPACITY_STAGE_ID');
const rate = positiveInteger('CAPACITY_RATE');
const duration = requiredEnv('CAPACITY_DURATION');
const preAllocatedVUs = positiveInteger('CAPACITY_PREALLOCATED_VUS');
const maxVUs = positiveInteger('CAPACITY_MAX_VUS');
const summaryPath = requiredEnv('CAPACITY_K6_SUMMARY');

const smallNote = 'n'.repeat(256);
const mediumNote = 'n'.repeat(4 * 1024);
const largeNote = 'n'.repeat(64 * 1024);

const acceptedOrders = new Counter('accepted_orders');
const orderAccepted = new Rate('order_accepted');

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate==0'],
    order_accepted: ['rate==1'],
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const sequence = exec.scenario.iterationInTest;
  const offeredAt = new Date().toISOString();
  const response = http.post(
    `${appURL}/orders`,
    JSON.stringify(orderFor(sequence)),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-GoMessenger-Capacity-Run': runID,
        'X-GoMessenger-Capacity-Stage': stageID,
        'X-GoMessenger-Capacity-Offered-At': offeredAt,
      },
      tags: { name: 'POST /orders' },
      timeout: '10s',
    },
  );
  const accepted = response.status === 202;
  acceptedOrders.add(accepted ? 1 : 0);
  orderAccepted.add(accepted);
  check(response, { 'order transaction committed and Outbox staged': (result) => result.status === 202 });
}

export function handleSummary(data) {
  return {
    [summaryPath]: JSON.stringify(data, null, 2),
    stdout: `k6 stage ${stageID}: iterations=${metricValue(data, 'iterations', 'count')} ` +
      `dropped=${metricValue(data, 'dropped_iterations', 'count')} ` +
      `accepted=${metricValue(data, 'accepted_orders', 'count')}\n`,
  };
}

function orderFor(sequence) {
  const bucket = sequence % 100;
  let itemCount = 1;
  let note = smallNote;
  if (bucket >= 95) {
    itemCount = 50;
    note = largeNote;
  } else if (bucket >= 80) {
    itemCount = 10;
    note = mediumNote;
  }
  const items = [];
  for (let index = 0; index < itemCount; index += 1) {
    items.push({
      sku: `SKU-${String(sequence % 1000).padStart(3, '0')}-${String(index).padStart(2, '0')}`,
      quantity: (index % 3) + 1,
      unitPrice: 100 + ((sequence + index) % 5000),
    });
  }
  return {
    orderId: `${runID}-${stageID}-${sequence}`,
    customerId: `customer-${String(sequence % 10000).padStart(4, '0')}`,
    currency: 'USD',
    items,
    note,
  };
}

function requiredEnv(name) {
  const value = __ENV[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positiveInteger(name) {
  const value = Number.parseInt(requiredEnv(name), 10);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function metricValue(data, metricName, valueName) {
  const metric = data.metrics[metricName];
  if (!metric || metric.values[valueName] === undefined) {
    return 0;
  }
  return metric.values[valueName];
}
