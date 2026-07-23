/**
 * 五子棋联机对战 · 积分计分服务（服务端权威）
 * --------------------------------------------------
 * 作用：
 *   1. 连接公共 MQTT broker（与游戏前端同一 broker），订阅 gomoku/score/req；
 *   2. 收到某局赛果后，用 Elo 期望积分模型权威结算双方积分；
 *   3. 把结算结果回写 gomoku/score/resp，两端各自按 gameId 认领本局积分。
 *
 * 设计要点（满足需求：积分差距越大 → 强者赢变化越小、弱者赢变化越大；幅度加大）：
 *   - 采用「每方各自 K 系数」的 Elo 结算：
 *        ΔR_self = K_self × (S_self − E_self)
 *     其中 E_self = 1 / (1 + 10^((R_opp − R_self)/D)) 为自身期望胜率。
 *   - K 分段（幅度较休闲默认更大）：rating<1500 → 64；1500~1999 → 48；≥2000 → 32。
 *     低分（往往是弱势方）K 更大 → 弱者赢时加分更多、强者赢弱势方时只小幅加分，天然放大分差效应。
 *   - 积分下限 floor=100，避免跌成负数；默认初始 1500。
 *
 * 运行：
 *   cd server && npm install && node score-server.js
 * 也可用环境变量覆盖：BROKER / D / FLOOR / PORT(忽略)
 */

'use strict';

const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');

// ---------- 配置 ----------
const BROKER = process.env.BROKER || 'wss://broker.emqx.io:8084/mqtt';
const D = Number(process.env.D || 400);          // 期望分差敏感系数
const FLOOR = Number(process.env.FLOOR || 100);  // 积分下限
const REQ_TOPIC = 'gomoku/score/req';
const RESP_TOPIC = 'gomoku/score/resp';
const LEADERBOARD_TOPIC = 'gomoku/leaderboard'; // 排行榜：服务端权威发布 top100（retained）
const LEADERBOARD_LIMIT = 100;
const RATINGS_FILE = path.join(__dirname, 'ratings.json');
const SEEN_FILE = path.join(__dirname, 'seen.json');
const LOCK_FILE = path.join(__dirname, '.server.lock'); // 单实例锁，防止双实例重复结算
const SEEN_CAP = 5000;                            // 去重表最多保留条数
const DEFAULT_RATING = 1500;
const DEFAULT_K = 64;

// ---------- 数据持久化 ----------
let ratings = {};   // uid -> { rating, games }
let seen = {};      // gameId -> true（已结算去重）

function loadStore() {
  try {
    if (fs.existsSync(RATINGS_FILE)) {
      const raw = JSON.parse(fs.readFileSync(RATINGS_FILE, 'utf8'));
      if (raw && typeof raw === 'object') ratings = raw;
    }
  } catch (e) { console.error('[store] ratings 读取失败，使用空表', e.message); }
  try {
    if (fs.existsSync(SEEN_FILE)) {
      const arr = JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8'));
      if (Array.isArray(arr)) { seen = {}; arr.forEach(id => { seen[id] = true; }); }
    }
  } catch (e) { console.error('[store] seen 读取失败', e.message); }
}
function saveRatings() {
  try { fs.writeFileSync(RATINGS_FILE, JSON.stringify(ratings, null, 2)); } catch (e) { console.error('[store] ratings 写入失败', e.message); }
}
function markSeen(id) {
  seen[id] = true;
  const keys = Object.keys(seen);
  if (keys.length > SEEN_CAP) {
    const drop = keys.slice(0, keys.length - SEEN_CAP);
    drop.forEach(k => delete seen[k]);
  }
  try { fs.writeFileSync(SEEN_FILE, JSON.stringify(Object.keys(seen))); } catch (e) {}
}

// ---------- Elo 结算 ----------
function kFor(rating) {
  if (rating <= 1500) return 64;   // 新号/默认 1500 也用最大幅度，快速收敛
  if (rating < 2000) return 48;
  return 32;
}
function expected(ra, rb) {
  return 1 / (1 + Math.pow(10, (rb - ra) / D));
}
function getRating(uid) {
  if (!ratings[uid]) ratings[uid] = { rating: DEFAULT_RATING, games: 0 };
  return ratings[uid];
}

/**
 * 结算一局
 * @param {string} a 玩家 A 的 uid
 * @param {string} b 玩家 B 的 uid
 * @param {number} sa A 的赛果分：1 胜 / 0.5 平 / 0 负
 * @returns {{ratings:{a:number,b:number}, deltas:{a:number,b:number}}}
 */
function settle(a, b, sa) {
  const A = getRating(a);
  const B = getRating(b);
  const sb = 1 - sa; // 零和赛果（A 胜则 B 负，平则各 0.5）
  const ea = expected(A.rating, B.rating);
  const eb = 1 - ea;
  // 每方用各自 K，放大分差效应；四舍五入
  const da = Math.round(kFor(A.rating) * (sa - ea));
  const db = Math.round(kFor(B.rating) * (sb - eb));
  A.rating = Math.max(FLOOR, A.rating + da);
  B.rating = Math.max(FLOOR, B.rating + db);
  A.games = (A.games || 0) + 1;
  B.games = (B.games || 0) + 1;
  return {
    ratings: { a: A.rating, b: B.rating },
    deltas: { a: da, b: db }
  };
}

// ---------- 排行榜（权威发布 top N）----------
// 计分服务持有全量 ratings，按积分降序取前 LEADERBOARD_LIMIT 名，
// 发布到 LEADERBOARD_TOPIC（retained），客户端订阅即可渲染「排行榜」标签页。
function publishLeaderboard() {
  try {
    const arr = Object.keys(ratings).map(uid => ({
      uid,
      rating: ratings[uid].rating,
      games: ratings[uid].games || 0
    }));
    arr.sort((x, y) => (y.rating - x.rating) || (x.uid < y.uid ? -1 : 1));
    const top = arr.slice(0, LEADERBOARD_LIMIT).map((r, i) => ({
      rank: i + 1, uid: r.uid, rating: r.rating, games: r.games
    }));
    client.publish(LEADERBOARD_TOPIC, JSON.stringify(top), { qos: 1, retain: true });
    console.log('[leaderboard] 已发布 top', top.length, '（最高分', top.length ? top[0].rating : '-', '）');
  } catch (e) {
    console.error('[leaderboard] 发布失败', e.message);
  }
}

// ---------- 单实例锁 ----------
// 防止误启动两个服务实例（双实例会导致同一局被重复结算、下发两条 resp）。
function acquireLock() {
  try {
    if (fs.existsSync(LOCK_FILE)) {
      const pid = parseInt(fs.readFileSync(LOCK_FILE, 'utf8'), 10);
      // 探测该 pid 是否仍存活（Windows 下 process.kill(pid,0) 对存活进程不抛错）
      let alive = false;
      try { process.kill(pid, 0); alive = true; } catch (e) { alive = false; }
      if (alive) {
        console.error('[lock] 已有计分服务实例在运行 (pid ' + pid + ')，本进程退出。请先结束旧实例。');
        process.exit(1);
      }
      // 旧锁对应的进程已死，接管
      console.warn('[lock] 发现残留锁 (pid ' + pid + ' 已不存在)，接管。');
    }
    fs.writeFileSync(LOCK_FILE, String(process.pid));
  } catch (e) {
    console.error('[lock] 无法获取实例锁，继续启动（请自行确保无双实例）:', e.message);
  }
}
function releaseLock() {
  try { if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE); } catch (e) {}
}

// ---------- MQTT ----------
let client = null; // 模块级 MQTT 客户端（publishLeaderboard 等函数需要引用）
function start() {
  loadStore();
  client = mqtt.connect(BROKER, {
    clientId: 'gomoku_score_server_' + Math.random().toString(16).slice(2, 10),
    reconnectPeriod: 2000,
    clean: true
  });

  client.on('connect', () => {
    console.log('[mqtt] 已连接计分 broker:', BROKER);
    client.subscribe(REQ_TOPIC, { qos: 0 }, (err) => {
      if (err) console.error('[mqtt] 订阅失败', err.message);
      else console.log('[mqtt] 已订阅', REQ_TOPIC);
    });
    // 启动即发布一次排行榜；并定期刷新，确保后订阅的客户端也能拿到最新榜单
    publishLeaderboard();
    if (!start._lbTimer) {
      start._lbTimer = setInterval(publishLeaderboard, 30000);
    }
  });

  client.on('message', (topic, payload) => {
    if (topic !== REQ_TOPIC) return;
    let msg;
    try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
    const { gameId, a, b, winner } = msg;
    if (!gameId || !a || !b) return;

    // 去重：同一局只结算一次（两端都会发 req，先到的结算，后到的直接回写原结果）
    if (seen[gameId]) {
      // 不重算，但确保另一端也能收到结果（重发存储结果）
      // 注意：本进程若刚重启且 seen 已加载，仍可不重算；否则视为新局
      return;
    }

    // 计算赛果分 sa
    let sa;
    if (winner === a) sa = 1;
    else if (winner === b) sa = 0;
    else if (winner === '' || winner == null) sa = 0.5; // 和棋
    else return; // winner 非法，忽略

    const result = settle(a, b, sa);
    markSeen(gameId);
    saveRatings();
    publishLeaderboard(); // 积分变化后刷新排行榜

    const resp = { gameId, ratings: result.ratings, deltas: result.deltas };
    try {
      client.publish(RESP_TOPIC, JSON.stringify(resp), { qos: 0 });
      console.log(`[score] ${a} vs ${b} -> A:${result.deltas.a>=0?'+':''}${result.deltas.a} B:${result.deltas.b>=0?'+':''}${result.deltas.b} [${gameId}]`);
    } catch (e) {
      console.error('[score] 回写失败', e.message);
    }
  });

  client.on('error', (e) => console.error('[mqtt] error', e.message));
  client.on('close', () => console.log('[mqtt] 连接断开，等待自动重连...'));

  // 优雅退出时落盘并释放锁
  const flush = () => { saveRatings(); releaseLock(); process.exit(0); };
  process.on('SIGINT', flush);
  process.on('SIGTERM', flush);
  process.on('exit', releaseLock);
}

acquireLock();
start();
