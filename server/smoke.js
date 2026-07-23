// 端到端冒烟测试：连接 broker，发赛果，收结算，验证去重
const mqtt = require('mqtt');
const BROKER = 'wss://broker.emqx.io:8084/mqtt';
const REQ = 'gomoku/score/req';
const RESP = 'gomoku/score/resp';

const c = mqtt.connect(BROKER, { clientId: 'smoke_' + Date.now(), clean: true, reconnectPeriod: 2000 });
let got = [];
let dupTimer;

c.on('connect', () => {
  console.log('[smoke] 已连接，订阅', RESP);
  c.subscribe(RESP, { qos: 0 }, () => {
    const gid = 'smoke_' + Date.now();
    const msg = { gameId: gid, a: 'alice', b: 'bob', winner: 'alice' };
    console.log('[smoke] 发布 req:', JSON.stringify(msg));
    c.publish(REQ, JSON.stringify(msg), { qos: 0 });
    // 重复发布同一局（验证去重：不应产生第 2 条 resp）
    setTimeout(() => c.publish(REQ, JSON.stringify(msg), { qos: 0 }), 400);
    // 5 秒超时结束
    setTimeout(() => finish(), 5000);
  });
});
c.on('message', (t, p) => {
  if (t !== RESP) return;
  let m; try { m = JSON.parse(p.toString()); } catch (e) { return; }
  got.push(m);
  console.log('[smoke] 收到 resp:', JSON.stringify(m));
});
c.on('error', (e) => console.error('[smoke] error', e.message));

function finish() {
  c.end(true);
  if (got.length === 1) console.log('\n✅ 通过：收到 1 条结算（重复发布已正确去重）');
  else if (got.length === 0) console.log('\n❌ 未收到任何结算（链路不通或服务未运行）');
  else console.log('\n⚠️ 收到 ' + got.length + ' 条结算（去重可能失效）');
  process.exit(0);
}
