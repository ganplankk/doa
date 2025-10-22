const crypto = require('crypto');
const ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
const SPACE = BigInt(ALPHABET.length);

function base62FromBigInt(n, length) {
  let s = '';
  for (let i = 0n; i < BigInt(length); i++) {
    s = ALPHABET[Number(n % SPACE)] + s;
    n = n / SPACE;
  }
  return s;
}

/**
 * 날짜(YYYYMMDD) + 시리얼 + salt → sha256 → 상위 36bit → base62(6)
 * 같은 날/같은 시리얼이면 같은 값(운영상 가독/추적에 유리)
 */
function generateHashedId6(serial, date = new Date(), salt = '') {
  const yyyy = date.getFullYear().toString();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const dayStr = `${yyyy}${mm}${dd}`;

  const normalized = String(serial ?? '').trim().toUpperCase();

  const h = crypto.createHash('sha256').update(`${dayStr}|${normalized}|${salt}`).digest();
  // 48bit → 상위 36bit
  let n = 0n;
  for (const b of h.subarray(0, 6)) n = (n << 8n) | BigInt(b);
  n = n >> 12n;

  const suffix = base62FromBigInt(n, 6);
  return `${dayStr}-${suffix}`;
}

module.exports = { generateHashedId6 };

