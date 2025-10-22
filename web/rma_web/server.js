// ===== 기본 모듈 =====
const express = require("express");
const path = require("path");
const { Pool } = require("pg");
const morgan = require("morgan");
const cors = require("cors");
const crypto = require("crypto");

// ===== 환경 설정 =====
const PORT = process.env.PORT || 8080;
const DB = {
  host: process.env.PGHOST || "pg-postgresql.data.svc.cluster.local",
  port: +(process.env.PGPORT || 5432),
  user: process.env.PGUSER || "rma_user",
  password: process.env.PGPASSWORD || "piolink.com1!",
  database: process.env.PGDATABASE || "doadb",
  max: +(process.env.PGPOOL_SIZE || 10),
  idleTimeoutMillis: 30000,
};

// ===== PostgreSQL Pool =====
const pool = new Pool(DB);
pool.on("error", (err) => {
  console.error("[pg] unexpected error on idle client", err);
  // 안전하게 종료
  setTimeout(() => process.exit(1), 10000);
});

// ===== Express App =====
const app = express();
app.use(morgan("combined"));
app.use(express.json({ limit: "1mb" }));
app.use(cors({ origin: true }));
app.use("/static", express.static(path.join(__dirname, "static")));

// ====== ID 생성 유틸 (rma + timestamp + random salt) ======
const ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const SPACE = BigInt(ALPHABET.length);

function base62FromBigInt(n, length) {
  let s = "";
  for (let i = 0n; i < BigInt(length); i++) {
    s = ALPHABET[Number(n % SPACE)] + s;
    n = n / SPACE;
  }
  return s;
}

function yyyymmddhhmmss(date = new Date()) {
  const yyyy = date.getFullYear().toString();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}${hh}${mi}${ss}`;
}

/**
 * 결과 예: rma-20251013145822-0aZ9Xk
 * - prefix: "rma-"
 * - timestamp: YYYYMMDDhhmmss
 * - suffix: sha256("rma|ts|" + random(16B) + "|" + pepper) 상위 36bit → base62(6)
 */
function generateRmaId(date = new Date(), pepper = process.env.ID_SALT || "") {
  const ts = yyyymmddhhmmss(date);
  const rnd = crypto.randomBytes(16);
  const h = crypto
    .createHash("sha256")
    .update("rma|" + ts + "|", "utf8")
    .update(rnd)
    .update("|" + pepper, "utf8")
    .digest();

  // 48bit → 상위 36bit 사용
  let n = 0n;
  for (const b of h.subarray(0, 6)) n = (n << 8n) | BigInt(b);
  n >>= 12n;

  const suffix = base62FromBigInt(n, 6);
  return `rma-${ts}-${suffix}`;
}

// ====== 헬스체크 ======
app.get("/healthz", (_req, res) => res.json({ ok: true }));

// ====== INSERT API ======
app.post("/api/rma", async (req, res) => {
  const b = req.body || {};

  const required = ["name", "company", "email", "model", "serial_number", "initial_install_date"];
  for (const k of required) {
    if (!b[k] || String(b[k]).trim() === "") {
      return res.status(400).json({ ok:false, error: `필수값 누락: ${k}` });
    }
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(b.email))) {
    return res.status(400).json({ ok:false, error: "이메일 형식이 올바르지 않습니다." });
  }

  const insertSQL = `
    INSERT INTO rma_lists
      (id, name, company, email, model, serial_number, as_method,
       initial_install_date, failure_date, version, memo, created_at)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, NOW())
    RETURNING id
  `;

  const baseParams = [
    null, // id 자리 (아래에서 주입)
    b.name?.trim(),
    b.company?.trim(),
    b.email?.trim(),
    b.model?.trim(),
    b.serial_number?.trim(),
    b.as_method || null,
    b.initial_install_date,
    b.failure_date || null,
    b.version || null,
    b.memo || null,
  ];

  const client = await pool.connect();
  try {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      const newId = generateRmaId(new Date(), process.env.ID_SALT || "");
      const params = [newId, ...baseParams.slice(1)];
      try {
        const r = await client.query(insertSQL, params);
        return res.json({ ok: true, id: r.rows[0]?.id, retries: attempt - 1 });
      } catch (err) {
        if (err && err.code === "23505" && attempt < maxRetries) {
          // unique_violation → 새 salt로 재시도
          continue;
        }
        console.error("[insert error]", err);
        return res.status(500).json({ ok:false, error: err.message || "DB 오류" });
      }
    }
  } finally {
    client.release();
  }
});

// ====== 정적 SPA 루트 ======
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// ====== 서버 시작 ======
app.listen(PORT, () => {
  console.log(`������ Server listening on :${PORT}`);
  console.log("DB config:", { ...DB, password: "***" });
});

