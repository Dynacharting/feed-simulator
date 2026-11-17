#!/usr/bin/env node
'use strict';

const { WebSocket } = require('ws');

const url = process.argv[2] || 'wss://feed-sim.v3m.xyz/feed';
const symbol = process.argv[3] || 'NEXO';

const RESET = '\x1b[0m';
const DIM = '\x1b[2m';
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const CYAN = '\x1b[36m';
const MAGENTA = '\x1b[35m';
const WHITE = '\x1b[37;1m';

const TYPE_COLOR = {
  stock_directory: CYAN,
  add_order: GREEN,
  add_order_mpid: GREEN,
  order_executed: YELLOW,
  order_cancel: RED,
  order_delete: RED,
  order_replace: MAGENTA,
  trade: WHITE,
  system_event: CYAN,
  stock_trading_action: MAGENTA,
};

// order_executed (E) carries no price, side or stock on the wire — faithful to
// ITCH, where an execution names only the resting order. So the viewer keeps
// its own map of the orders it has seen added, and prints each execution at
// that order's price, with the aggressor (the side opposite the resting
// order). An order that was resting before this connection opened has never
// been seen, and its execution prints with '?' for price and side.
const orders = new Map(); // orderRef -> { side, price, shares }

function track(msg) {
  switch (msg.type) {
    case 'add_order':
    case 'add_order_mpid':
      orders.set(msg.orderRef, { side: msg.side, price: msg.price, shares: msg.shares });
      break;
    case 'order_replace': {
      const o = orders.get(msg.origOrderRef);
      orders.delete(msg.origOrderRef);
      if (o) orders.set(msg.orderRef, { side: o.side, price: msg.price, shares: msg.shares });
      break;
    }
    case 'order_executed':
    case 'order_cancel': {
      const o = orders.get(msg.orderRef);
      if (o && (o.shares -= msg.shares) <= 0) orders.delete(msg.orderRef);
      break;
    }
    case 'order_delete':
      orders.delete(msg.orderRef);
      break;
  }
}

function fmtPrice(p) {
  return p != null ? Number(p).toFixed(2).padStart(10) : '       N/A';
}

function fmt(msg) {
  const type = msg.type || 'unknown';
  const color = TYPE_COLOR[type] || DIM;
  const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
  const parts = [`${DIM}${ts}${RESET}`, `${color}${type.padEnd(18)}${RESET}`];

  switch (type) {
    case 'stock_directory':
      parts.push(`${msg.stock}`);
      break;
    case 'add_order':
    case 'add_order_mpid':
      parts.push(side(msg.side), `${CYAN}${String(msg.shares).padStart(6)}${RESET}`, '@', `${YELLOW}${fmtPrice(msg.price)}${RESET}`);
      if (msg.mpid) parts.push(`${DIM}${msg.mpid}${RESET}`);
      break;
    case 'order_executed': {
      const o = orders.get(msg.orderRef);
      const aggressor = o ? (o.side === 'B' ? 'S' : 'B') : null;
      parts.push(aggressor ? side(aggressor) : `${DIM}   ?${RESET}`, `${CYAN}${String(msg.shares).padStart(6)}${RESET}`, '@', `${YELLOW}${fmtPrice(o && o.price)}${RESET}`, `${DIM}match=${msg.matchNumber}${RESET}`);
      break;
    }
    case 'order_cancel':
      parts.push(`${CYAN}${String(msg.shares).padStart(6)}${RESET}`, 'cancelled');
      break;
    case 'order_delete':
      parts.push(`ref=${msg.orderRef}`);
      break;
    case 'order_replace':
      parts.push(`${msg.origOrderRef}->${msg.orderRef}`, `${CYAN}${String(msg.shares).padStart(6)}${RESET}`, '@', `${YELLOW}${fmtPrice(msg.price)}${RESET}`);
      break;
    case 'trade':
      parts.push(side(msg.side), `${CYAN}${String(msg.shares).padStart(6)}${RESET}`, '@', `${YELLOW}${fmtPrice(msg.price)}${RESET}`);
      break;
    default:
      parts.push(JSON.stringify(msg));
  }

  return parts.join(' ');
}

function side(s) {
  return s === 'B' ? `${GREEN} BUY${RESET}` : `${RED}SELL${RESET}`;
}

console.log(`connecting to ${url} | symbol: ${symbol}`);

const ws = new WebSocket(url);

ws.on('open', () => {
  console.log(`${GREEN}connected${RESET} - streaming ${symbol}\n`);
  ws.send(JSON.stringify({ action: 'subscribe', symbols: [symbol] }));
});

ws.on('message', (data) => {
  const msg = JSON.parse(data);
  console.log(fmt(msg)); // format first: an execution prints at its order's pre-fill state
  track(msg);
});

ws.on('close', () => {
  console.log(`\n${RED}disconnected${RESET}`);
  process.exit(0);
});

ws.on('error', (err) => {
  console.error(`${RED}error: ${err.message}${RESET}`);
  process.exit(1);
});

process.on('SIGINT', () => {
  ws.close();
});
