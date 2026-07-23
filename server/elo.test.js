// 纯逻辑测试：复刻 score-server.js 的 Elo 公式，验证幅度特性
// 运行：node elo.test.js
'use strict';
const D = 400;
const FLOOR = 100;
function kFor(rating) {
  if (rating <= 1500) return 64;
  if (rating < 2000) return 48;
  return 32;
}
function expected(ra, rb) { return 1 / (1 + Math.pow(10, (rb - ra) / D)); }

function row(ra, rb) {
  const ea = expected(ra, rb);
  const eb = 1 - ea;
  // A 胜（sa=1），用各自 K
  const daWin = Math.round(kFor(ra) * (1 - ea));
  const dbLose = Math.round(kFor(rb) * (0 - eb));
  // B 胜（A 负），即 A 输
  const daLose = Math.round(kFor(ra) * (0 - ea));
  const dbWin = Math.round(kFor(rb) * (1 - eb));
  return { ea: ea.toFixed(2), aWin: daWin, bLose: dbLose, aLose: daLose, bWin: dbWin };
}

const cases = [
  [1500, 1500],
  [1700, 1500], // A 强
  [1500, 1700], // A 弱
  [1900, 1500],
  [1500, 1900],
  [2100, 1600],
  [1600, 2100],
];
console.log('场景                  A期望胜率  强者赢(A+)  弱方输(B-)  强者输(A-)  弱者赢(B+)');
for (const [a, b] of cases) {
  const r = row(a, b);
  const label = `${a} vs ${b}`;
  console.log(
    label.padEnd(20),
    String(r.ea).padStart(6),
    String(r.aWin).padStart(10),
    String(r.bLose).padStart(10),
    String(r.aLose).padStart(10),
    String(r.bWin).padStart(10)
  );
}
console.log('\n验证：强者赢(A+) 应远小于 弱者赢(B+)；强者输(A-) 应大于 弱者输(B-)。');
