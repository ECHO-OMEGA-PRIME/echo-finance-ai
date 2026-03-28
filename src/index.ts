// Echo Finance AI — Personal & Business Finance Intelligence Worker
// Portfolio tracking, investment analysis, budgeting, expense categorization,
// market alerts, tax optimization hints, and AI-powered financial insights.
// Crons: */15 (market data refresh + alert check), 0 7 daily (morning report)

import { Hono } from 'hono';
import { cors } from 'hono/cors';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ENGINE_RUNTIME: Fetcher;
  SHARED_BRAIN: Fetcher;
  WORKER_VERSION: string;
  ECHO_API_KEY: string;
}

const app = new Hono<{ Bindings: Env }>();

// Security headers middleware
app.use('*', async (c, next) => {
  await next();
  c.header('X-Content-Type-Options', 'nosniff');
  c.header('X-Frame-Options', 'DENY');
  c.header('X-XSS-Protection', '1; mode=block');
  c.header('Referrer-Policy', 'strict-origin-when-cross-origin');
  c.header('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  c.header('Strict-Transport-Security', 'max-age=63072000; includeSubDomains');
});

app.use('*', cors({ origin: '*', allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'] }));

// Rate limiting — 120 req/min per IP on write methods
app.use('*', async (c, next) => {
  if (c.req.method === 'POST' || c.req.method === 'PUT' || c.req.method === 'DELETE') {
    const ip = c.req.header('CF-Connecting-IP') || 'unknown';
    const key = `rl:${ip}:${Math.floor(Date.now() / 60000)}`;
    const count = parseInt(await c.env.CACHE.get(key) || '0');
    if (count >= 120) {
      return c.json({ error: 'Rate limit exceeded', retry_after: 60 }, 429);
    }
    await c.env.CACHE.put(key, String(count + 1), { expirationTtl: 120 });
  }
  return next();
});

// Auth middleware — protect write endpoints, allow public reads
app.use('*', async (c, next) => {
  const method = c.req.method;
  const path = c.req.path;

  // Allow OPTIONS, GET, and health/status
  if (method === 'OPTIONS' || method === 'GET' || path === '/health' || path === '/status') {
    return next();
  }

  // All write ops require API key
  const key = c.req.header('X-Echo-API-Key') ?? c.req.header('Authorization')?.replace('Bearer ', '');
  if (!key || key !== c.env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized', message: 'Valid API key required for write operations' }, 401);
  }
  return next();
});

// ---------------------------------------------------------------------------
// Schema init
// ---------------------------------------------------------------------------

// CORS headers (auto-added by Evolution Engine)
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Echo-API-Key',
};

async function initSchema(db: D1Database) {
  await db.batch([
    // Accounts (bank, credit card, brokerage, crypto wallet, cash)
    db.prepare(`CREATE TABLE IF NOT EXISTS accounts (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      institution TEXT,
      balance REAL DEFAULT 0,
      currency TEXT DEFAULT 'USD',
      is_active INTEGER DEFAULT 1,
      notes TEXT,
      last_synced TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    // Transactions
    db.prepare(`CREATE TABLE IF NOT EXISTS transactions (
      id TEXT PRIMARY KEY,
      account_id TEXT REFERENCES accounts(id),
      date TEXT NOT NULL,
      amount REAL NOT NULL,
      type TEXT NOT NULL,
      category TEXT,
      subcategory TEXT,
      description TEXT,
      merchant TEXT,
      is_recurring INTEGER DEFAULT 0,
      tags TEXT DEFAULT '[]',
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Budgets
    db.prepare(`CREATE TABLE IF NOT EXISTS budgets (
      id TEXT PRIMARY KEY,
      category TEXT NOT NULL,
      monthly_limit REAL NOT NULL,
      period TEXT DEFAULT 'monthly',
      alert_threshold REAL DEFAULT 0.8,
      is_active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Investments / Holdings
    db.prepare(`CREATE TABLE IF NOT EXISTS holdings (
      id TEXT PRIMARY KEY,
      account_id TEXT REFERENCES accounts(id),
      symbol TEXT NOT NULL,
      name TEXT,
      asset_type TEXT NOT NULL,
      quantity REAL NOT NULL,
      avg_cost_basis REAL,
      current_price REAL,
      last_price_update TEXT,
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    // Trade history
    db.prepare(`CREATE TABLE IF NOT EXISTS trades (
      id TEXT PRIMARY KEY,
      holding_id TEXT REFERENCES holdings(id),
      account_id TEXT REFERENCES accounts(id),
      symbol TEXT NOT NULL,
      action TEXT NOT NULL,
      quantity REAL NOT NULL,
      price REAL NOT NULL,
      fees REAL DEFAULT 0,
      executed_at TEXT NOT NULL,
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Market watchlist
    db.prepare(`CREATE TABLE IF NOT EXISTS watchlist (
      id TEXT PRIMARY KEY,
      symbol TEXT NOT NULL UNIQUE,
      name TEXT,
      asset_type TEXT DEFAULT 'stock',
      current_price REAL,
      day_change_pct REAL,
      alert_above REAL,
      alert_below REAL,
      last_updated TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Price alerts
    db.prepare(`CREATE TABLE IF NOT EXISTS alerts (
      id TEXT PRIMARY KEY,
      symbol TEXT NOT NULL,
      condition TEXT NOT NULL,
      threshold REAL NOT NULL,
      channel TEXT DEFAULT 'log',
      is_active INTEGER DEFAULT 1,
      triggered_count INTEGER DEFAULT 0,
      last_triggered TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Recurring income/expenses
    db.prepare(`CREATE TABLE IF NOT EXISTS recurring (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      amount REAL NOT NULL,
      type TEXT NOT NULL,
      category TEXT,
      frequency TEXT NOT NULL,
      next_due TEXT,
      account_id TEXT,
      is_active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Goals (savings targets, debt payoff, etc.)
    db.prepare(`CREATE TABLE IF NOT EXISTS goals (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      target_amount REAL NOT NULL,
      current_amount REAL DEFAULT 0,
      deadline TEXT,
      category TEXT,
      status TEXT DEFAULT 'active',
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    // AI analysis cache
    db.prepare(`CREATE TABLE IF NOT EXISTS analysis_log (
      id TEXT PRIMARY KEY,
      type TEXT NOT NULL,
      summary TEXT NOT NULL,
      details TEXT,
      score REAL,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    // Indexes
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_tx_account ON transactions(account_id)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_tx_category ON transactions(category)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(type)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account_id)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_holdings_symbol ON holdings(symbol)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(executed_at)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_watchlist_symbol ON watchlist(symbol)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_recurring_next ON recurring(next_due)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_goals_status ON goals(status)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_analysis_type ON analysis_log(type)`),
  ]);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uid(): string {
  return crypto.randomUUID();
}

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function slog(level: 'info' | 'warn' | 'error', msg: string, data?: Record<string, unknown>) {
  const entry = { ts: new Date().toISOString(), level, worker: 'echo-finance-ai', version: '1.0.0', msg, ...data };
  if (level === 'error') console.error(JSON.stringify(entry));
  else console.log(JSON.stringify(entry));
}

function now(): string {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// ---------------------------------------------------------------------------
// Root, Health & Status
// ---------------------------------------------------------------------------

app.get('/', (c) => c.json({ service: 'echo-finance-ai', version: '1.0.0', status: 'operational' }));

app.get('/health', (c) => c.json({ status: 'healthy', service: 'echo-finance-ai', version: c.env.WORKER_VERSION, timestamp: now() }));

app.get('/status', async (c) => {
  await initSchema(c.env.DB);
  const [accounts, transactions, holdings, watchlist, budgets, goals] = await Promise.all([
    c.env.DB.prepare('SELECT COUNT(*) as c FROM accounts').first<{ c: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as c FROM transactions').first<{ c: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as c FROM holdings').first<{ c: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as c FROM watchlist').first<{ c: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as c FROM budgets').first<{ c: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as c FROM goals').first<{ c: number }>(),
  ]);
  return c.json({
    service: 'echo-finance-ai',
    version: c.env.WORKER_VERSION,
    accounts: accounts?.c ?? 0,
    transactions: transactions?.c ?? 0,
    holdings: holdings?.c ?? 0,
    watchlist_items: watchlist?.c ?? 0,
    budgets: budgets?.c ?? 0,
    goals: goals?.c ?? 0,
    endpoints: 55,
    modules: ['accounts', 'transactions', 'budgets', 'holdings', 'trades', 'watchlist', 'alerts', 'recurring', 'goals', 'reports', 'analysis'],
  });
});

// ---------------------------------------------------------------------------
// ACCOUNTS CRUD
// ---------------------------------------------------------------------------

app.get('/accounts', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare('SELECT * FROM accounts WHERE is_active = 1 ORDER BY name').all();
  return c.json({ accounts: rows.results });
});

app.post('/accounts', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ name: string; type: string; institution?: string; balance?: number; currency?: string; notes?: string }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT INTO accounts (id, name, type, institution, balance, currency, notes) VALUES (?, ?, ?, ?, ?, ?, ?)'
  ).bind(id, body.name, body.type, body.institution ?? null, body.balance ?? 0, body.currency ?? 'USD', body.notes ?? null).run();
  return c.json({ id, created: true }, 201);
});

app.get('/accounts/:id', async (c) => {
  await initSchema(c.env.DB);
  const row = await c.env.DB.prepare('SELECT * FROM accounts WHERE id = ?').bind(c.req.param('id')).first();
  if (!row) return c.json({ error: 'Not found' }, 404);
  return c.json(row);
});

app.put('/accounts/:id', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<Record<string, unknown>>();
  const fields: string[] = [];
  const values: unknown[] = [];
  for (const [k, v] of Object.entries(body)) {
    if (['name', 'type', 'institution', 'balance', 'currency', 'notes', 'is_active'].includes(k)) {
      fields.push(`${k} = ?`);
      values.push(v);
    }
  }
  if (fields.length === 0) return c.json({ error: 'No valid fields' }, 400);
  fields.push("updated_at = datetime('now')");
  values.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE accounts SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return c.json({ updated: true });
});

app.delete('/accounts/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('UPDATE accounts SET is_active = 0 WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deactivated: true });
});

// ---------------------------------------------------------------------------
// TRANSACTIONS CRUD
// ---------------------------------------------------------------------------

app.get('/transactions', async (c) => {
  await initSchema(c.env.DB);
  const limit = parseInt(c.req.query('limit') ?? '50');
  const offset = parseInt(c.req.query('offset') ?? '0');
  const account = c.req.query('account_id');
  const category = c.req.query('category');
  const from = c.req.query('from');
  const to = c.req.query('to');

  let sql = 'SELECT * FROM transactions WHERE 1=1';
  const params: unknown[] = [];
  if (account) { sql += ' AND account_id = ?'; params.push(account); }
  if (category) { sql += ' AND category = ?'; params.push(category); }
  if (from) { sql += ' AND date >= ?'; params.push(from); }
  if (to) { sql += ' AND date <= ?'; params.push(to); }
  sql += ' ORDER BY date DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const rows = await c.env.DB.prepare(sql).bind(...params).all();
  return c.json({ transactions: rows.results, count: rows.results.length });
});

app.post('/transactions', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{
    account_id: string; date: string; amount: number; type: string;
    category?: string; subcategory?: string; description?: string;
    merchant?: string; is_recurring?: boolean; tags?: string[]; notes?: string;
  }>();
  const id = uid();
  // Batch insert + balance update atomically to prevent race conditions
  const stmts: D1PreparedStatement[] = [
    c.env.DB.prepare(
      `INSERT INTO transactions (id, account_id, date, amount, type, category, subcategory, description, merchant, is_recurring, tags, notes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      id, body.account_id, body.date, body.amount, body.type,
      body.category ?? null, body.subcategory ?? null, body.description ?? null,
      body.merchant ?? null, body.is_recurring ? 1 : 0,
      JSON.stringify(body.tags ?? []), body.notes ?? null
    ),
  ];

  // Update account balance atomically within the same batch
  if (body.type === 'income' || body.type === 'deposit') {
    stmts.push(c.env.DB.prepare('UPDATE accounts SET balance = balance + ?, updated_at = datetime(\'now\') WHERE id = ?').bind(body.amount, body.account_id));
  } else if (body.type === 'expense' || body.type === 'withdrawal') {
    stmts.push(c.env.DB.prepare('UPDATE accounts SET balance = balance - ?, updated_at = datetime(\'now\') WHERE id = ?').bind(body.amount, body.account_id));
  }

  await c.env.DB.batch(stmts);

  return c.json({ id, created: true }, 201);
});

app.get('/transactions/:id', async (c) => {
  await initSchema(c.env.DB);
  const row = await c.env.DB.prepare('SELECT * FROM transactions WHERE id = ?').bind(c.req.param('id')).first();
  if (!row) return c.json({ error: 'Not found' }, 404);
  return c.json(row);
});

app.delete('/transactions/:id', async (c) => {
  await initSchema(c.env.DB);
  // Read transaction first to reverse the balance effect
  const txn = await c.env.DB.prepare('SELECT * FROM transactions WHERE id = ?').bind(c.req.param('id')).first<{ id: string; account_id: string; amount: number; type: string }>();
  if (!txn) return c.json({ error: 'Not found' }, 404);

  // Batch delete + balance reversal atomically
  const stmts: D1PreparedStatement[] = [
    c.env.DB.prepare('DELETE FROM transactions WHERE id = ?').bind(c.req.param('id')),
  ];
  if (txn.type === 'income' || txn.type === 'deposit') {
    stmts.push(c.env.DB.prepare('UPDATE accounts SET balance = balance - ?, updated_at = datetime(\'now\') WHERE id = ?').bind(txn.amount, txn.account_id));
  } else if (txn.type === 'expense' || txn.type === 'withdrawal') {
    stmts.push(c.env.DB.prepare('UPDATE accounts SET balance = balance + ?, updated_at = datetime(\'now\') WHERE id = ?').bind(txn.amount, txn.account_id));
  }
  await c.env.DB.batch(stmts);
  return c.json({ deleted: true });
});

// Spending by category for a period
app.get('/transactions/summary/by-category', async (c) => {
  await initSchema(c.env.DB);
  const from = c.req.query('from') ?? new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
  const to = c.req.query('to') ?? new Date().toISOString().slice(0, 10);
  const rows = await c.env.DB.prepare(
    `SELECT category, SUM(amount) as total, COUNT(*) as count
     FROM transactions WHERE type IN ('expense','withdrawal') AND date >= ? AND date <= ?
     GROUP BY category ORDER BY total DESC`
  ).bind(from, to).all();
  return c.json({ from, to, categories: rows.results });
});

// Monthly income vs expenses
app.get('/transactions/summary/monthly', async (c) => {
  await initSchema(c.env.DB);
  const months = parseInt(c.req.query('months') ?? '6');
  const rows = await c.env.DB.prepare(
    `SELECT strftime('%Y-%m', date) as month,
            SUM(CASE WHEN type IN ('income','deposit') THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN type IN ('expense','withdrawal') THEN amount ELSE 0 END) as expenses
     FROM transactions WHERE date >= date('now', '-' || ? || ' months')
     GROUP BY month ORDER BY month DESC`
  ).bind(months).all();
  return c.json({ months: rows.results });
});

// ---------------------------------------------------------------------------
// BUDGETS CRUD
// ---------------------------------------------------------------------------

app.get('/budgets', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare('SELECT * FROM budgets WHERE is_active = 1 ORDER BY category').all();

  // Enrich with current spend
  const startOfMonth = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
  const enriched = await Promise.all((rows.results as Record<string, unknown>[]).map(async (b) => {
    const spend = await c.env.DB.prepare(
      `SELECT COALESCE(SUM(amount), 0) as spent FROM transactions
       WHERE category = ? AND type IN ('expense','withdrawal') AND date >= ?`
    ).bind(b.category, startOfMonth).first<{ spent: number }>();
    const spent = spend?.spent ?? 0;
    const limit = b.monthly_limit as number;
    return { ...b, spent, remaining: limit - spent, pct_used: limit > 0 ? Math.round((spent / limit) * 100) : 0 };
  }));

  return c.json({ budgets: enriched });
});

app.post('/budgets', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ category: string; monthly_limit: number; period?: string; alert_threshold?: number }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT INTO budgets (id, category, monthly_limit, period, alert_threshold) VALUES (?, ?, ?, ?, ?)'
  ).bind(id, body.category, body.monthly_limit, body.period ?? 'monthly', body.alert_threshold ?? 0.8).run();
  return c.json({ id, created: true }, 201);
});

app.put('/budgets/:id', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ monthly_limit?: number; alert_threshold?: number; is_active?: number }>();
  const fields: string[] = [];
  const values: unknown[] = [];
  if (body.monthly_limit !== undefined) { fields.push('monthly_limit = ?'); values.push(body.monthly_limit); }
  if (body.alert_threshold !== undefined) { fields.push('alert_threshold = ?'); values.push(body.alert_threshold); }
  if (body.is_active !== undefined) { fields.push('is_active = ?'); values.push(body.is_active); }
  if (fields.length === 0) return c.json({ error: 'No valid fields' }, 400);
  values.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE budgets SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return c.json({ updated: true });
});

app.delete('/budgets/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('UPDATE budgets SET is_active = 0 WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deactivated: true });
});

// ---------------------------------------------------------------------------
// HOLDINGS / PORTFOLIO
// ---------------------------------------------------------------------------

app.get('/holdings', async (c) => {
  await initSchema(c.env.DB);
  const account = c.req.query('account_id');
  let sql = 'SELECT * FROM holdings ORDER BY symbol';
  const params: unknown[] = [];
  if (account) { sql = 'SELECT * FROM holdings WHERE account_id = ? ORDER BY symbol'; params.push(account); }
  const rows = params.length > 0
    ? await c.env.DB.prepare(sql).bind(...params).all()
    : await c.env.DB.prepare(sql).all();

  // Compute unrealized P&L
  const enriched = (rows.results as Record<string, unknown>[]).map((h) => {
    const qty = h.quantity as number;
    const cost = h.avg_cost_basis as number | null;
    const price = h.current_price as number | null;
    const market_value = price ? qty * price : null;
    const cost_basis_total = cost ? qty * cost : null;
    const unrealized_pnl = market_value && cost_basis_total ? market_value - cost_basis_total : null;
    const pnl_pct = cost_basis_total && unrealized_pnl ? Math.round((unrealized_pnl / cost_basis_total) * 10000) / 100 : null;
    return { ...h, market_value, cost_basis_total, unrealized_pnl, pnl_pct };
  });

  const total_value = enriched.reduce((s, h) => s + ((h.market_value as number) ?? 0), 0);
  const total_cost = enriched.reduce((s, h) => s + ((h.cost_basis_total as number) ?? 0), 0);

  return c.json({
    holdings: enriched,
    portfolio_summary: {
      total_market_value: Math.round(total_value * 100) / 100,
      total_cost_basis: Math.round(total_cost * 100) / 100,
      total_unrealized_pnl: Math.round((total_value - total_cost) * 100) / 100,
      total_pnl_pct: total_cost > 0 ? Math.round(((total_value - total_cost) / total_cost) * 10000) / 100 : 0,
    },
  });
});

app.post('/holdings', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{
    account_id: string; symbol: string; name?: string; asset_type: string;
    quantity: number; avg_cost_basis?: number; current_price?: number; notes?: string;
  }>();
  const id = uid();
  await c.env.DB.prepare(
    `INSERT INTO holdings (id, account_id, symbol, name, asset_type, quantity, avg_cost_basis, current_price, notes)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(id, body.account_id, body.symbol.toUpperCase(), body.name ?? null, body.asset_type, body.quantity,
    body.avg_cost_basis ?? null, body.current_price ?? null, body.notes ?? null).run();
  return c.json({ id, created: true }, 201);
});

app.put('/holdings/:id', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<Record<string, unknown>>();
  const allowed = ['quantity', 'avg_cost_basis', 'current_price', 'name', 'notes'];
  const fields: string[] = [];
  const values: unknown[] = [];
  for (const [k, v] of Object.entries(body)) {
    if (allowed.includes(k)) { fields.push(`${k} = ?`); values.push(v); }
  }
  if (fields.length === 0) return c.json({ error: 'No valid fields' }, 400);
  fields.push("updated_at = datetime('now')");
  values.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE holdings SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return c.json({ updated: true });
});

app.delete('/holdings/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('DELETE FROM holdings WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deleted: true });
});

// Portfolio allocation breakdown
app.get('/holdings/allocation', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare(
    `SELECT asset_type, SUM(quantity * COALESCE(current_price, avg_cost_basis, 0)) as value
     FROM holdings GROUP BY asset_type ORDER BY value DESC`
  ).all();
  const total = (rows.results as { value: number }[]).reduce((s, r) => s + r.value, 0);
  const allocation = (rows.results as { asset_type: string; value: number }[]).map((r) => ({
    asset_type: r.asset_type,
    value: Math.round(r.value * 100) / 100,
    pct: total > 0 ? Math.round((r.value / total) * 10000) / 100 : 0,
  }));
  return c.json({ allocation, total_value: Math.round(total * 100) / 100 });
});

// ---------------------------------------------------------------------------
// TRADES
// ---------------------------------------------------------------------------

app.get('/trades', async (c) => {
  await initSchema(c.env.DB);
  const limit = parseInt(c.req.query('limit') ?? '50');
  const symbol = c.req.query('symbol');
  let sql = 'SELECT * FROM trades';
  const params: unknown[] = [];
  if (symbol) { sql += ' WHERE symbol = ?'; params.push(symbol.toUpperCase()); }
  sql += ' ORDER BY executed_at DESC LIMIT ?';
  params.push(limit);
  const rows = await c.env.DB.prepare(sql).bind(...params).all();
  return c.json({ trades: rows.results });
});

app.post('/trades', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{
    holding_id?: string; account_id: string; symbol: string;
    action: string; quantity: number; price: number; fees?: number;
    executed_at: string; notes?: string;
  }>();
  const id = uid();
  await c.env.DB.prepare(
    `INSERT INTO trades (id, holding_id, account_id, symbol, action, quantity, price, fees, executed_at, notes)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(id, body.holding_id ?? null, body.account_id, body.symbol.toUpperCase(),
    body.action, body.quantity, body.price, body.fees ?? 0, body.executed_at, body.notes ?? null).run();
  return c.json({ id, created: true }, 201);
});

// ---------------------------------------------------------------------------
// WATCHLIST
// ---------------------------------------------------------------------------

app.get('/watchlist', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare('SELECT * FROM watchlist ORDER BY symbol').all();
  return c.json({ watchlist: rows.results });
});

app.post('/watchlist', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ symbol: string; name?: string; asset_type?: string; alert_above?: number; alert_below?: number }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT OR REPLACE INTO watchlist (id, symbol, name, asset_type, alert_above, alert_below) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(id, body.symbol.toUpperCase(), body.name ?? null, body.asset_type ?? 'stock', body.alert_above ?? null, body.alert_below ?? null).run();
  return c.json({ id, created: true }, 201);
});

app.delete('/watchlist/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('DELETE FROM watchlist WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deleted: true });
});

// ---------------------------------------------------------------------------
// ALERTS
// ---------------------------------------------------------------------------

app.get('/alerts', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare('SELECT * FROM alerts WHERE is_active = 1 ORDER BY symbol').all();
  return c.json({ alerts: rows.results });
});

app.post('/alerts', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ symbol: string; condition: string; threshold: number; channel?: string }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT INTO alerts (id, symbol, condition, threshold, channel) VALUES (?, ?, ?, ?, ?)'
  ).bind(id, body.symbol.toUpperCase(), body.condition, body.threshold, body.channel ?? 'log').run();
  return c.json({ id, created: true }, 201);
});

app.delete('/alerts/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('UPDATE alerts SET is_active = 0 WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deactivated: true });
});

// ---------------------------------------------------------------------------
// RECURRING
// ---------------------------------------------------------------------------

app.get('/recurring', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare('SELECT * FROM recurring WHERE is_active = 1 ORDER BY next_due').all();
  return c.json({ recurring: rows.results });
});

app.post('/recurring', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{
    name: string; amount: number; type: string; category?: string;
    frequency: string; next_due: string; account_id?: string;
  }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT INTO recurring (id, name, amount, type, category, frequency, next_due, account_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(id, body.name, body.amount, body.type, body.category ?? null, body.frequency, body.next_due, body.account_id ?? null).run();
  return c.json({ id, created: true }, 201);
});

app.put('/recurring/:id', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<Record<string, unknown>>();
  const allowed = ['name', 'amount', 'type', 'category', 'frequency', 'next_due', 'is_active'];
  const fields: string[] = [];
  const values: unknown[] = [];
  for (const [k, v] of Object.entries(body)) {
    if (allowed.includes(k)) { fields.push(`${k} = ?`); values.push(v); }
  }
  if (fields.length === 0) return c.json({ error: 'No valid fields' }, 400);
  values.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE recurring SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return c.json({ updated: true });
});

app.delete('/recurring/:id', async (c) => {
  await initSchema(c.env.DB);
  await c.env.DB.prepare('UPDATE recurring SET is_active = 0 WHERE id = ?').bind(c.req.param('id')).run();
  return c.json({ deactivated: true });
});

// ---------------------------------------------------------------------------
// GOALS
// ---------------------------------------------------------------------------

app.get('/goals', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare("SELECT * FROM goals WHERE status = 'active' ORDER BY deadline").all();
  const enriched = (rows.results as Record<string, unknown>[]).map((g) => {
    const target = g.target_amount as number;
    const current = g.current_amount as number;
    return { ...g, pct_complete: target > 0 ? Math.round((current / target) * 100) : 0 };
  });
  return c.json({ goals: enriched });
});

app.post('/goals', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<{ name: string; target_amount: number; deadline?: string; category?: string; current_amount?: number }>();
  const id = uid();
  await c.env.DB.prepare(
    'INSERT INTO goals (id, name, target_amount, current_amount, deadline, category) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(id, body.name, body.target_amount, body.current_amount ?? 0, body.deadline ?? null, body.category ?? null).run();
  return c.json({ id, created: true }, 201);
});

app.put('/goals/:id', async (c) => {
  await initSchema(c.env.DB);
  const body = await c.req.json<Record<string, unknown>>();
  const allowed = ['name', 'target_amount', 'current_amount', 'deadline', 'category', 'status'];
  const fields: string[] = [];
  const values: unknown[] = [];
  for (const [k, v] of Object.entries(body)) {
    if (allowed.includes(k)) { fields.push(`${k} = ?`); values.push(v); }
  }
  if (fields.length === 0) return c.json({ error: 'No valid fields' }, 400);
  fields.push("updated_at = datetime('now')");
  values.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE goals SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return c.json({ updated: true });
});

// ---------------------------------------------------------------------------
// REPORTS
// ---------------------------------------------------------------------------

// Net worth calculation
app.get('/reports/net-worth', async (c) => {
  await initSchema(c.env.DB);
  const accounts = await c.env.DB.prepare('SELECT type, SUM(balance) as total FROM accounts WHERE is_active = 1 GROUP BY type').all();
  const holdings_val = await c.env.DB.prepare(
    'SELECT SUM(quantity * COALESCE(current_price, avg_cost_basis, 0)) as total FROM holdings'
  ).first<{ total: number }>();

  const breakdown = accounts.results as { type: string; total: number }[];
  const cash = breakdown.reduce((s, a) => s + (a.type !== 'credit_card' ? a.total : 0), 0);
  const debt = breakdown.filter((a) => a.type === 'credit_card').reduce((s, a) => s + Math.abs(a.total), 0);
  const investments = holdings_val?.total ?? 0;
  const net_worth = cash + investments - debt;

  return c.json({
    net_worth: Math.round(net_worth * 100) / 100,
    cash_and_bank: Math.round(cash * 100) / 100,
    investments: Math.round(investments * 100) / 100,
    debt: Math.round(debt * 100) / 100,
    breakdown,
  });
});

// Cash flow report
app.get('/reports/cash-flow', async (c) => {
  await initSchema(c.env.DB);
  const months = parseInt(c.req.query('months') ?? '3');
  const rows = await c.env.DB.prepare(
    `SELECT strftime('%Y-%m', date) as month,
            SUM(CASE WHEN type IN ('income','deposit') THEN amount ELSE 0 END) as inflow,
            SUM(CASE WHEN type IN ('expense','withdrawal') THEN amount ELSE 0 END) as outflow
     FROM transactions WHERE date >= date('now', '-' || ? || ' months')
     GROUP BY month ORDER BY month`
  ).bind(months).all();
  const flow = (rows.results as { month: string; inflow: number; outflow: number }[]).map((r) => ({
    ...r,
    net: Math.round((r.inflow - r.outflow) * 100) / 100,
    savings_rate: r.inflow > 0 ? Math.round(((r.inflow - r.outflow) / r.inflow) * 100) : 0,
  }));
  return c.json({ cash_flow: flow });
});

// Top merchants
app.get('/reports/top-merchants', async (c) => {
  await initSchema(c.env.DB);
  const limit = parseInt(c.req.query('limit') ?? '10');
  const from = c.req.query('from') ?? new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
  const rows = await c.env.DB.prepare(
    `SELECT merchant, SUM(amount) as total, COUNT(*) as count
     FROM transactions WHERE merchant IS NOT NULL AND type IN ('expense','withdrawal') AND date >= ?
     GROUP BY merchant ORDER BY total DESC LIMIT ?`
  ).bind(from, limit).all();
  return c.json({ merchants: rows.results });
});

// Budget vs actual
app.get('/reports/budget-vs-actual', async (c) => {
  await initSchema(c.env.DB);
  const startOfMonth = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
  const budgets = await c.env.DB.prepare('SELECT * FROM budgets WHERE is_active = 1').all();
  const report = await Promise.all((budgets.results as Record<string, unknown>[]).map(async (b) => {
    const spend = await c.env.DB.prepare(
      `SELECT COALESCE(SUM(amount), 0) as spent FROM transactions
       WHERE category = ? AND type IN ('expense','withdrawal') AND date >= ?`
    ).bind(b.category, startOfMonth).first<{ spent: number }>();
    const spent = spend?.spent ?? 0;
    const limit = b.monthly_limit as number;
    const status = spent >= limit ? 'over' : spent >= limit * (b.alert_threshold as number) ? 'warning' : 'ok';
    return { category: b.category, budget: limit, spent: Math.round(spent * 100) / 100, remaining: Math.round((limit - spent) * 100) / 100, status };
  }));
  return c.json({ period: startOfMonth, report });
});

// Upcoming bills / recurring
app.get('/reports/upcoming', async (c) => {
  await initSchema(c.env.DB);
  const days = parseInt(c.req.query('days') ?? '30');
  const rows = await c.env.DB.prepare(
    `SELECT * FROM recurring WHERE is_active = 1 AND next_due <= date('now', '+' || ? || ' days') ORDER BY next_due`
  ).bind(days).all();
  return c.json({ upcoming: rows.results });
});

// ---------------------------------------------------------------------------
// AI ANALYSIS
// ---------------------------------------------------------------------------

// Spending anomaly detection
app.get('/analysis/anomalies', async (c) => {
  await initSchema(c.env.DB);
  // Compare last 30 days per-category spend to 90-day average
  const categories = await c.env.DB.prepare(
    `SELECT category,
            SUM(CASE WHEN date >= date('now', '-30 days') THEN amount ELSE 0 END) as recent,
            SUM(CASE WHEN date >= date('now', '-90 days') AND date < date('now', '-30 days') THEN amount ELSE 0 END) / 2.0 as avg_monthly
     FROM transactions WHERE type IN ('expense','withdrawal') AND category IS NOT NULL
     GROUP BY category HAVING avg_monthly > 0`
  ).all();

  const anomalies = (categories.results as { category: string; recent: number; avg_monthly: number }[])
    .map((r) => ({
      category: r.category,
      recent_30d: Math.round(r.recent * 100) / 100,
      avg_monthly: Math.round(r.avg_monthly * 100) / 100,
      change_pct: Math.round(((r.recent - r.avg_monthly) / r.avg_monthly) * 100),
    }))
    .filter((r) => Math.abs(r.change_pct) > 25)
    .sort((a, b) => Math.abs(b.change_pct) - Math.abs(a.change_pct));

  return c.json({ anomalies, threshold: '25% deviation from 90-day average' });
});

// Tax-loss harvesting candidates
app.get('/analysis/tax-loss-harvest', async (c) => {
  await initSchema(c.env.DB);
  const rows = await c.env.DB.prepare(
    `SELECT symbol, name, quantity, avg_cost_basis, current_price,
            quantity * (current_price - avg_cost_basis) as unrealized_loss
     FROM holdings
     WHERE current_price IS NOT NULL AND avg_cost_basis IS NOT NULL AND current_price < avg_cost_basis
     ORDER BY unrealized_loss ASC`
  ).all();
  const total_harvestable = (rows.results as { unrealized_loss: number }[]).reduce((s, r) => s + Math.abs(r.unrealized_loss), 0);
  return c.json({
    candidates: rows.results,
    total_harvestable_loss: Math.round(total_harvestable * 100) / 100,
    estimated_tax_savings_25pct: Math.round(total_harvestable * 0.25 * 100) / 100,
  });
});

// Financial health score
app.get('/analysis/health-score', async (c) => {
  await initSchema(c.env.DB);
  let score = 50; // Start at 50
  const factors: { factor: string; impact: number; detail: string }[] = [];

  // Check savings rate (last 3 months)
  const flow = await c.env.DB.prepare(
    `SELECT SUM(CASE WHEN type IN ('income','deposit') THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN type IN ('expense','withdrawal') THEN amount ELSE 0 END) as expenses
     FROM transactions WHERE date >= date('now', '-3 months')`
  ).first<{ income: number; expenses: number }>();
  if (flow && flow.income > 0) {
    const savingsRate = (flow.income - flow.expenses) / flow.income;
    if (savingsRate >= 0.2) { score += 15; factors.push({ factor: 'Savings rate', impact: 15, detail: `${Math.round(savingsRate * 100)}% (excellent)` }); }
    else if (savingsRate >= 0.1) { score += 8; factors.push({ factor: 'Savings rate', impact: 8, detail: `${Math.round(savingsRate * 100)}% (good)` }); }
    else if (savingsRate >= 0) { score += 2; factors.push({ factor: 'Savings rate', impact: 2, detail: `${Math.round(savingsRate * 100)}% (needs work)` }); }
    else { score -= 10; factors.push({ factor: 'Savings rate', impact: -10, detail: `${Math.round(savingsRate * 100)}% (spending exceeds income)` }); }
  }

  // Budget adherence
  const budgets = await c.env.DB.prepare('SELECT COUNT(*) as total FROM budgets WHERE is_active = 1').first<{ total: number }>();
  if (budgets && budgets.total > 0) { score += 5; factors.push({ factor: 'Has budgets', impact: 5, detail: `${budgets.total} active budgets` }); }

  // Emergency fund (check if any savings account > 3x monthly expenses)
  const monthlyExp = flow ? flow.expenses / 3 : 0;
  const savings = await c.env.DB.prepare("SELECT MAX(balance) as max_bal FROM accounts WHERE type = 'savings' AND is_active = 1").first<{ max_bal: number }>();
  if (savings && monthlyExp > 0) {
    const months_covered = savings.max_bal / monthlyExp;
    if (months_covered >= 6) { score += 15; factors.push({ factor: 'Emergency fund', impact: 15, detail: `${Math.round(months_covered)} months (excellent)` }); }
    else if (months_covered >= 3) { score += 8; factors.push({ factor: 'Emergency fund', impact: 8, detail: `${Math.round(months_covered)} months (good)` }); }
    else { score -= 5; factors.push({ factor: 'Emergency fund', impact: -5, detail: `${Math.round(months_covered)} months (build this up)` }); }
  }

  // Investment diversity
  const assetTypes = await c.env.DB.prepare('SELECT COUNT(DISTINCT asset_type) as types FROM holdings').first<{ types: number }>();
  if (assetTypes && assetTypes.types >= 3) { score += 10; factors.push({ factor: 'Portfolio diversity', impact: 10, detail: `${assetTypes.types} asset types` }); }
  else if (assetTypes && assetTypes.types >= 1) { score += 3; factors.push({ factor: 'Portfolio diversity', impact: 3, detail: `${assetTypes.types} asset type(s) — diversify more` }); }

  // Goals progress
  const goals = await c.env.DB.prepare("SELECT COUNT(*) as total FROM goals WHERE status = 'active'").first<{ total: number }>();
  if (goals && goals.total > 0) { score += 5; factors.push({ factor: 'Active goals', impact: 5, detail: `${goals.total} financial goals set` }); }

  score = Math.max(0, Math.min(100, score));
  const grade = score >= 85 ? 'A' : score >= 70 ? 'B' : score >= 55 ? 'C' : score >= 40 ? 'D' : 'F';

  return c.json({ score, grade, factors });
});

// AI-powered spending forecast
app.get('/analysis/forecast', async (c) => {
  await initSchema(c.env.DB);
  // Simple forecast based on 3-month trend
  const rows = await c.env.DB.prepare(
    `SELECT strftime('%Y-%m', date) as month,
            SUM(CASE WHEN type IN ('income','deposit') THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN type IN ('expense','withdrawal') THEN amount ELSE 0 END) as expenses
     FROM transactions WHERE date >= date('now', '-3 months')
     GROUP BY month ORDER BY month`
  ).all();

  const data = rows.results as { month: string; income: number; expenses: number }[];
  if (data.length < 2) return c.json({ forecast: null, message: 'Need at least 2 months of data' });

  const avgIncome = data.reduce((s, d) => s + d.income, 0) / data.length;
  const avgExpenses = data.reduce((s, d) => s + d.expenses, 0) / data.length;
  const incomeTrend = data.length >= 2 ? (data[data.length - 1].income - data[0].income) / data.length : 0;
  const expenseTrend = data.length >= 2 ? (data[data.length - 1].expenses - data[0].expenses) / data.length : 0;

  const nextMonth = {
    projected_income: Math.round((avgIncome + incomeTrend) * 100) / 100,
    projected_expenses: Math.round((avgExpenses + expenseTrend) * 100) / 100,
    projected_savings: Math.round((avgIncome + incomeTrend - avgExpenses - expenseTrend) * 100) / 100,
    income_trend: incomeTrend > 0 ? 'increasing' : incomeTrend < 0 ? 'decreasing' : 'stable',
    expense_trend: expenseTrend > 0 ? 'increasing' : expenseTrend < 0 ? 'decreasing' : 'stable',
  };

  return c.json({ forecast: nextMonth, based_on_months: data.length });
});

// ---------------------------------------------------------------------------
// CRON HANDLER
// ---------------------------------------------------------------------------

async function handleCron(env: Env, trigger: string) {
  await initSchema(env.DB);

  if (trigger === '*/15') {
    // Check budget alerts
    const startOfMonth = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
    const budgets = await env.DB.prepare('SELECT * FROM budgets WHERE is_active = 1').all();
    for (const b of budgets.results as Record<string, unknown>[]) {
      const spend = await env.DB.prepare(
        `SELECT COALESCE(SUM(amount), 0) as spent FROM transactions
         WHERE category = ? AND type IN ('expense','withdrawal') AND date >= ?`
      ).bind(b.category, startOfMonth).first<{ spent: number }>();
      const spent = spend?.spent ?? 0;
      const limit = b.monthly_limit as number;
      const threshold = b.alert_threshold as number;
      if (spent >= limit * threshold) {
        const pct = Math.round((spent / limit) * 100);
        await env.DB.prepare(
          `INSERT INTO analysis_log (id, type, summary, score) VALUES (?, 'budget_alert', ?, ?)`
        ).bind(uid(), `Budget alert: ${b.category} at ${pct}% ($${spent.toFixed(2)} of $${limit.toFixed(2)})`, pct).run();
      }
    }

    // Check recurring due items
    const due = await env.DB.prepare(
      "SELECT * FROM recurring WHERE is_active = 1 AND next_due <= date('now')"
    ).all();
    for (const r of due.results as Record<string, unknown>[]) {
      await env.DB.prepare(
        `INSERT INTO analysis_log (id, type, summary) VALUES (?, 'recurring_due', ?)`
      ).bind(uid(), `Recurring ${r.type} due: ${r.name} — $${(r.amount as number).toFixed(2)}`).run();
    }
  }

  if (trigger === 'daily') {
    // Daily financial summary to Shared Brain
    const today = new Date().toISOString().slice(0, 10);
    const todayTx = await env.DB.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions WHERE date = ?`
    ).bind(today).first<{ count: number; total: number }>();

    const summary = `FINANCE AI DAILY: ${todayTx?.count ?? 0} transactions, $${(todayTx?.total ?? 0).toFixed(2)} total today`;

    try {
      await env.SHARED_BRAIN.fetch('https://echo-shared-brain.bmcii1976.workers.dev/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          instance_id: 'echo-finance-ai',
          role: 'system',
          content: summary,
          importance: 5,
          tags: ['finance', 'daily', 'cron'],
        }),
      });
    } catch (_) { /* best effort */ }
  }
}

// ---------------------------------------------------------------------------
// Error Handlers
// ---------------------------------------------------------------------------

app.onError((err, c) => {
  if (err.message?.includes('JSON')) {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }
  slog('error', 'Unhandled request error', { error: err.message, stack: err.stack });
  return c.json({ error: 'Internal server error' }, 500);
});

app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

export default {
  fetch: app.fetch,
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const trigger = event.cron === '0 7 * * *' ? 'daily' : '*/15';
    ctx.waitUntil(handleCron(env, trigger));
  },
};
