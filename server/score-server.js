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
 * 环境变量（均可选）：BROKER / D / FLOOR / GITHUB_TOKEN / GITHUB_OWNER /
 *   GITHUB_REPO / GITHUB_USERS_PATH / GITHUB_BRANCH
 *   设置 GITHUB_TOKEN 后，每局结算会把「积分直接写入库中每位用户的个人数据」
 *   （仓库 ${GITHUB_USERS_PATH}，结构为 uid -> {uid,nick,avatar,rating,games,wins}）。
 *   客户端读取排行榜时，依据该文件中的用户个人信息数据刷新排行榜。
 *   未设置 GITHUB_TOKEN 时，仅本地 + MQTT 保留（排行榜仍可经 MQTT 消费）。
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
const LEADERBOARD_TOPIC = 'gomoku/leaderboard'; // 实时榜单：服务端直接推送 top100 用户数据（retained）；GitHub(data/users.json) 仅作持久化备份
const PROFILE_SET_TOPIC = 'gomoku/profile/set'; // 客户端实时上报个人资料（昵称/头像）变化；服务端即时写入仓库个人用户数据
const LEADERBOARD_LIMIT = 100;
// GitHub 持久化：把「每位用户的个人数据（含积分）」同步进仓库，排行榜即依据这些数据刷新
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const GITHUB_OWNER = process.env.GITHUB_OWNER || 'yanghaoyv0917';
const GITHUB_REPO = process.env.GITHUB_REPO || 'gomoku';
const GITHUB_USERS_PATH = process.env.GITHUB_USERS_PATH || 'data/users.json';
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';
const GH_API = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents`;
const USERS_FILE = path.join(__dirname, 'users.json');
const SEEN_FILE = path.join(__dirname, 'seen.json');
const LOCK_FILE = path.join(__dirname, '.server.lock'); // 单实例锁，防止双实例重复结算
const SEEN_CAP = 5000;                            // 去重表最多保留条数
const DEFAULT_RATING = 1500;
const DEFAULT_K = 64;

// ---------- 数据持久化 ----------
// 个人用户数据：uid -> { uid, nick, avatar, rating, games, wins }
let users = {};
let seen = {};      // gameId -> true（已结算去重）

function loadStore() {
  try {
    if (fs.existsSync(USERS_FILE)) {
      const raw = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
      if (raw && typeof raw === 'object') users = raw;
    }
  } catch (e) { console.error('[store] users 读取失败，使用空表', e.message); }
  try {
    if (fs.existsSync(SEEN_FILE)) {
      const arr = JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8'));
      if (Array.isArray(arr)) { seen = {}; arr.forEach(id => { seen[id] = true; }); }
    }
  } catch (e) { console.error('[store] seen 读取失败', e.message); }
}
function saveUsers() {
  try { fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2)); } catch (e) { console.error('[store] users 写入失败', e.message); }
  scheduleGithubPush();
  publishLeaderboard(); // 任何用户数据变化都实时推送给客户端
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
function getProfile(uid) {
  if (!users[uid]) users[uid] = { uid: uid, nick: uid, avatar: null, rating: DEFAULT_RATING, games: 0, wins: 0 };
  return users[uid];
}

/**
 * 结算一局，并把积分直接写入两位玩家的个人用户数据
 * @param {string} a 玩家 A 的 uid
 * @param {string} b 玩家 B 的 uid
 * @param {number} sa A 的赛果分：1 胜 / 0.5 平 / 0 负
 * @param {{nick?:string,avatar?:string}} aMeta A 的昵称/头像（客户端上报）
 * @param {{nick?:string,avatar?:string}} bMeta B 的昵称/头像（客户端上报，可能为 undefined）
 * @returns {{ratings:{a:number,b:number}, deltas:{a:number,b:number}}}
 */
function settle(a, b, sa, aMeta, bMeta) {
  const A = getProfile(a);
  const B = getProfile(b);
  // 用客户端上报的昵称/头像补全个人用户数据（仅在有值时覆盖，避免回退成 uid）
  if (aMeta) {
    if (aMeta.nick) A.nick = aMeta.nick;
    if (aMeta.avatar) A.avatar = aMeta.avatar;
  }
  if (bMeta) {
    if (bMeta.nick) B.nick = bMeta.nick;
    if (bMeta.avatar) B.avatar = bMeta.avatar;
  }
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
  if (sa === 1) A.wins = (A.wins || 0) + 1;
  if (sb === 1) B.wins = (B.wins || 0) + 1;
  return {
    ratings: { a: A.rating, b: B.rating },
    deltas: { a: da, b: db }
  };
}

/**
 * 实时处理客户端上报的个人资料变化（昵称/头像）。
 * 客户端在「修改昵称」或「登录」时向 gomoku/profile/set 发布 {uid,nick,avatar}，
 * 服务端据此更新该用户的个人数据，并即时写入仓库（排行榜据此刷新）。
 * 注意：头像若为体积较大的 data: URL（本地上传图片）则忽略，避免 MQTT/仓库膨胀；
 *       该情况下头像仍会在对局结算时由客户端随赛果一并上报。
 */
function handleProfileSet(payload) {
  let msg;
  try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
  const uid = msg && msg.uid;
  if (!uid) return;
  const p = getProfile(uid);
  let changed = false;
  if (msg.nick && typeof msg.nick === 'string') {
    const nick = msg.nick.trim().slice(0, 64);
    if (nick && nick !== p.nick) { p.nick = nick; changed = true; }
  }
  if (typeof msg.avatar === 'string' && msg.avatar.length > 0 && msg.avatar.length <= 256 && msg.avatar !== p.avatar) {
    p.avatar = msg.avatar; changed = true;
  }
  if (!changed) return;
  saveUsers();          // 写本地 + 触发近实时入库 + 实时推送榜单
  console.log('[profile] 已实时更新个人资料', uid, '->', p.nick);
}

// ---------- 实时排行榜 ----------
// 每次用户数据变化，直接把 top100 榜单数据通过 MQTT 推给客户端（retained）。
// 客户端收到即渲染，无需再回源 GitHub。GitHub(data/users.json) 仅作持久化备份。
function publishLeaderboard() {
  if (!client || !client.connected) return; // 未连接时不发布（如 bootstrap 阶段）
  try {
    const all = Object.keys(users).map(uid => {
      const u = users[uid] || {};
      // 头像仅在「短字符串（emoji/小图）」时随榜推送，避免 data:URL 撑大 MQTT 消息
      const avatar = (typeof u.avatar === 'string' && u.avatar.length <= 32) ? u.avatar : '';
      return { uid, nick: u.nick || uid, avatar, rating: u.rating || 0, games: u.games || 0, wins: u.wins || 0 };
    }).sort((a, b) => (b.rating - a.rating) || (a.uid < b.uid ? -1 : 1));
    const top = all.slice(0, 100);
    const payload = JSON.stringify({ updated: true, ts: Date.now(), count: all.length, users: top });
    client.publish(LEADERBOARD_TOPIC, payload, { qos: 1, retain: true });
    console.log('[leaderboard] 已发布实时榜单（共', all.length, '名，推送 top', top.length, '）');
  } catch (e) {
    console.error('[leaderboard] 发布失败', e.message);
  }
}

// ---------- GitHub 入库（个人用户数据，排行榜数据源）----------
// 把全量 users（每位用户的个人数据，含积分）同步进仓库的 data/users.json。
// 积分被「直接写入」每位用户的个人数据中；客户端读取排行榜时依据该文件刷新。
let ghPushTimer = null;
let ghDirty = false;

function ghHeaders() {
  return {
    'Authorization': `Bearer ${GITHUB_TOKEN}`,
    'User-Agent': 'gomoku-score-server',
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json'
  };
}

async function pushUsersToGithub() {
  if (!GITHUB_TOKEN) {
    console.warn('[github] 未设置 GITHUB_TOKEN 环境变量，跳过仓库同步（仅本地 + MQTT 保留）。');
    return;
  }
  try {
    const content = JSON.stringify(users, null, 2);
    const b64 = Buffer.from(content, 'utf8').toString('base64');
    // 取已有 sha（文件可能尚不存在）
    let sha = null;
    try {
      const r = await fetch(`${GH_API}/${GITHUB_USERS_PATH}?ref=${GITHUB_BRANCH}`, { headers: ghHeaders() });
      if (r.ok) { const j = await r.json(); sha = j.sha; }
    } catch (e) { /* 文件不存在时忽略 */ }
    const body = { message: 'chore: 同步用户个人数据(含积分) users.json', content: b64, branch: GITHUB_BRANCH };
    if (sha) body.sha = sha;
    const r = await fetch(`${GH_API}/${GITHUB_USERS_PATH}`, {
      method: 'PUT', headers: ghHeaders(), body: JSON.stringify(body)
    });
    if (r.ok) console.log('[github] 已同步 users.json 到仓库', `(${Object.keys(users).length} 名用户)`);
    else { const t = await r.text(); console.error('[github] 同步失败', r.status, t.slice(0, 200)); }
  } catch (e) {
    console.error('[github] 同步异常', e.message);
  }
}

// 实时入库：单条变更在 ~GH_COALESCE_MS 内落地仓库；突发多次变更在该窗口内合并，避免高频打 GitHub API。
// 既保证“用户个人资料（昵称/积分）一变化就写入库”，又不会因对局洪峰触发限流。
const GH_COALESCE_MS = 1000; // 合并窗口（毫秒）
let ghLastPushTs = 0;
function scheduleGithubPush() {
  ghDirty = true;
  if (ghPushTimer) return;
  const sinceLast = Date.now() - ghLastPushTs;
  const wait = sinceLast >= GH_COALESCE_MS ? 0 : (GH_COALESCE_MS - sinceLast);
  ghPushTimer = setTimeout(async () => {
    ghPushTimer = null;
    if (ghDirty) {
      ghDirty = false;
      ghLastPushTs = Date.now();
      try { await pushUsersToGithub(); } catch (e) { console.error('[github] push 异常', e.message); }
    }
  }, wait);
}

// 启动引导：本地空表时，从仓库 data/users.json 拉取作为权威初始数据（GitHub 为真相源）。
// 若不同步到本地，首次结算 pushUsersToGithub 会用空表覆盖仓库、清空其他玩家数据，故必须先把仓库拉全。
// raw 常被沙箱/网络拦截，故 raw 失败再用 GitHub Contents API 兜底（与客户端一致）。
async function bootstrapFromGithub() {
  if (Object.keys(users).length > 0) return;
  const tryParse = (obj) => (obj && typeof obj === 'object' && !Array.isArray(obj));
  // 1) raw（无需鉴权，最快）
  try {
    const r = await fetch(`https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/${GITHUB_USERS_PATH}`, { signal: AbortSignal.timeout(5000) });
    if (r.ok) {
      const obj = await r.json();
      if (tryParse(obj)) { users = obj; saveUsers(); console.log('[github] 从仓库(raw)引导 users 数据', Object.keys(users).length, '条'); return; }
    }
  } catch (e) { console.warn('[github] raw 引导失败，尝试 API', e.message); }
  // 2) GitHub Contents API 兜底
  try {
    const r = await fetch(`${GH_API}/${GITHUB_USERS_PATH}?ref=${GITHUB_BRANCH}`, { headers: ghHeaders(), signal: AbortSignal.timeout(5000) });
    if (r.ok) {
      const j = await r.json();
      if (j && j.content) {
        const obj = JSON.parse(Buffer.from(j.content, 'base64').toString('utf8'));
        if (tryParse(obj)) { users = obj; saveUsers(); console.log('[github] 从仓库(API)引导 users 数据', Object.keys(users).length, '条'); return; }
      }
    }
  } catch (e) { console.warn('[github] API 引导失败（使用本地空表）', e.message); }
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
async function start() {
  loadStore();
  await bootstrapFromGithub();
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
    client.subscribe(PROFILE_SET_TOPIC, { qos: 0 }, (err) => {
      if (err) console.error('[mqtt] 订阅失败', err.message);
      else console.log('[mqtt] 已订阅', PROFILE_SET_TOPIC);
    });
    // 启动即发布一次排行榜；并定期刷新，确保后订阅的客户端也能拿到最新榜单
    publishLeaderboard();
    if (!start._lbTimer) {
      start._lbTimer = setInterval(publishLeaderboard, 30000);
    }
  });

  client.on('message', (topic, payload) => {
    if (topic === PROFILE_SET_TOPIC) { handleProfileSet(payload); return; }
    if (topic !== REQ_TOPIC) return;
    let msg;
    try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
    const { gameId, a, b, winner, aNick, aAvatar, bNick, bAvatar } = msg;
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

    const result = settle(a, b, sa,
      { nick: aNick, avatar: aAvatar },
      (bNick || bAvatar) ? { nick: bNick, avatar: bAvatar } : undefined
    );
    markSeen(gameId);
    saveUsers(); // 写本地 + 触发近实时入库 + 实时推送榜单

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

  // 优雅退出时落盘并释放锁（确保 GitHub 同步完成）
  const flush = async () => {
    saveUsers();
    if (ghDirty) { ghDirty = false; await pushUsersToGithub(); }
    releaseLock();
    process.exit(0);
  };
  process.on('SIGINT', () => { flush(); });
  process.on('SIGTERM', () => { flush(); });
  process.on('exit', releaseLock);
}

acquireLock();
start();
