#!/usr/bin/env python3
import os, sys, signal, time, json, threading
from typing import Dict, List, Any, Tuple, Optional
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========== Env ==========
BOOTSTRAP   = os.environ["KAFKA_BOOTSTRAP"]
TOPIC       = os.environ["KAFKA_TOPIC"]
SHEET_ID    = os.environ["SHEET_ID"]
SHEET_NAME  = os.environ.get("SHEET_NAME", "Sheet1")
DLQ_TOPIC   = os.environ.get("DLQ_TOPIC", "")  # optional

BATCH_SIZE  = int(os.environ.get("BATCH_SIZE", "200"))
POLL_MS     = int(os.environ.get("POLL_MS", "1000"))
METRICS_PORT= int(os.environ.get("METRICS_PORT", "8000"))

PK_HEADER   = "__pk"                # 업서트 인덱스 컬럼(시트용)
TS_HEADER   = "__source_ts_ms"      # 최신성 판단용(시트에도 보관)
REQUIRED_HEADERS = [PK_HEADER, "id", "name", "email", "__op", "__deleted", TS_HEADER]  # 필요에 맞게

# ========== Metrics ==========
m_msgs_in      = Counter("cdc_msgs_in_total", "Messages polled from Kafka")
m_msgs_ok      = Counter("cdc_msgs_ok_total", "Messages applied to Sheets successfully")
m_msgs_fail    = Counter("cdc_msgs_fail_total", "Messages failed to process")
m_msgs_skip    = Counter("cdc_msgs_skipped_total", "Messages skipped (stale/missing pk)")
m_updates      = Counter("cdc_sheet_updates_total", "Rows updated in sheet")
m_appends      = Counter("cdc_sheet_appends_total", "Rows appended in sheet")
m_deletes      = Counter("cdc_sheet_deletes_total", "Rows deleted from sheet")
m_api_calls    = Counter("cdc_sheets_api_calls_total", "Google Sheets API calls")
m_batch_latency= Histogram("cdc_batch_seconds", "Batch end-to-end seconds")
m_rows_cached  = Gauge("cdc_sheet_rows", "Row count cached from sheet")

# ========== Google Sheets ==========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def build_sheets():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

sheets = build_sheets()

def get_sheet_id(spreadsheet_id: str, title: str) -> int:
    m_api_calls.inc()
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == title:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"sheet '{title}' not found")

def a1_col(col_idx_0: int) -> str:
    # 0-based column index -> A, B, ..., AA ...
    col = ""
    i = col_idx_0
    while True:
        i, r = divmod(i, 26)
        col = chr(ord('A') + r) + col
        if i == 0: break
        i -= 1
    return col

def a1(col_idx_0: int, row_1: int) -> str:
    return f"{a1_col(col_idx_0)}{row_1}"

def get_headers_and_map() -> Tuple[List[str], Dict[str,int]]:
    m_api_calls.inc()
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!1:1"
    ).execute()
    headers = res.get("values", [[]])
    headers = headers[0] if headers else []
    return headers, {h: i for i, h in enumerate(headers)}

def ensure_headers(required: List[str]) -> Tuple[List[str], Dict[str,int]]:
    headers, h2c = get_headers_and_map()
    changed = False
    if not headers:
        headers = required[:]
        h2c = {h:i for i,h in enumerate(headers)}
        m_api_calls.inc()
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:{a1(len(headers)-1,1)}",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()
        changed = True
    else:
        # append missing headers to the right
        for h in required:
            if h not in h2c:
                headers.append(h)
                h2c[h] = len(headers)-1
                changed = True
        if changed:
            m_api_calls.inc()
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:{a1(len(headers)-1,1)}",
                valueInputOption="RAW",
                body={"values":[headers]}
            ).execute()
    return headers, h2c

def protect_header(sheet_id: int):
    try:
        m_api_calls.inc()
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests":[{
                "addProtectedRange":{
                    "protectedRange":{
                        "range":{"sheetId":sheet_id,"startRowIndex":0,"endRowIndex":1},
                        "description":"Lock header row",
                        "warningOnly": True
                    }
                }
            }]}
        ).execute()
    except HttpError:
        pass

def hide_column(sheet_id: int, col_idx_0: int):
    try:
        m_api_calls.inc()
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests":[{
                "updateDimensionProperties":{
                    "range":{"sheetId":sheet_id,"dimension":"COLUMNS",
                             "startIndex":col_idx_0,"endIndex":col_idx_0+1},
                    "properties":{"hiddenByUser": True},
                    "fields":"hiddenByUser"
                }
            }]}
        ).execute()
    except HttpError:
        pass

def load_pk_and_ts(pk_col_idx: int, ts_col_idx: Optional[int]) -> Tuple[Dict[str,int], Dict[str,int]]:
    """
    returns:
      pk_map:  __pk -> row_index(1-based)
      last_ts: __pk -> last_ts_ms_from_sheet (int; missing -> -1)
    """
    # Read PK column (A2:A) and TS column if present
    col_letter_pk = a1_col(pk_col_idx)
    rngs = [f"{SHEET_NAME}!{col_letter_pk}2:{col_letter_pk}"]
    if ts_col_idx is not None:
        col_letter_ts = a1_col(ts_col_idx)
        rngs.append(f"{SHEET_NAME}!{col_letter_ts}2:{col_letter_ts}")
    m_api_calls.inc()
    res = sheets.spreadsheets().values().batchGet(
        spreadsheetId=SHEET_ID, ranges=rngs, majorDimension="COLUMNS"
    ).execute()
    value_ranges = res.get("valueRanges", [])
    pks = value_ranges[0].get("values", [[]])
    pks = pks[0] if pks else []
    pk_map: Dict[str,int] = {}
    for i, pk in enumerate(pks, start=2):
        if pk:
            pk_map[str(pk)] = i
    last_ts: Dict[str,int] = {}
    if ts_col_idx is not None and len(value_ranges) > 1:
        ts_vals = value_ranges[1].get("values", [[]])
        ts_vals = ts_vals[0] if ts_vals else []
        for i, ts in enumerate(ts_vals, start=2):
            pk = pks[i-2] if i-2 < len(pks) else None
            if pk:
                try:
                    last_ts[str(pk)] = int(ts)
                except Exception:
                    last_ts[str(pk)] = -1
    m_rows_cached.set(len(pk_map))
    return pk_map, last_ts

# ========== Kafka ==========
def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "sheets-upserter",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
        "allow.auto.create.topics": False,
    })

producer = Producer({"bootstrap.servers": BOOTSTRAP}) if DLQ_TOPIC else None

def send_dlq(msg, reason: str):
    if not producer:
        return
    try:
        payload = {
            "reason": reason,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8") if msg.value() else None,
            "timestamp": msg.timestamp()[1],
        }
        producer.produce(DLQ_TOPIC, json.dumps(payload).encode("utf-8"))
        producer.flush(3)
    except Exception as e:
        print(f"[dlq-fail] {e}", file=sys.stderr)

# ========== State & Apply ==========
class SheetState:
    def __init__(self):
        self.sheet_id = get_sheet_id(SHEET_ID, SHEET_NAME)
        self.headers, self.h2c = ensure_headers(REQUIRED_HEADERS)
        if PK_HEADER not in self.h2c:
            raise RuntimeError(f"Sheet must contain header '{PK_HEADER}'")
        hide_column(self.sheet_id, self.h2c[PK_HEADER])         # __pk 숨김(옵션)
        protect_header(self.sheet_id)                           # 헤더 보호(경고)
        # 캐시 로드
        self.pk_map, self.sheet_ts = load_pk_and_ts(self.h2c[PK_HEADER], self.h2c.get(TS_HEADER))
        # 메모리 최신성 가드(재시작에도 안전하려면 sheet_ts를 사용, 메모리는 보조)
        self.mem_ts: Dict[str,int] = dict(self.sheet_ts)
        self.ncols = len(self.headers)

    def build_row_values(self, rec: Dict[str,Any]) -> List[str]:
        row = [""] * self.ncols
        for h, col in self.h2c.items():
            if h == PK_HEADER:
                continue
            v = rec.get(h, "")
            row[col] = "" if v is None else str(v)
        return row

def is_network_error(e: Exception) -> bool:
    transient = (HttpError, ConnectionError, TimeoutError, BrokenPipeError)
    return isinstance(e, transient)

@retry(wait=wait_exponential(multiplier=1, min=1, max=30),
       stop=stop_after_attempt(6),
       retry=retry_if_exception_type((HttpError, ConnectionError, TimeoutError, BrokenPipeError)))
def apply_batch(state: SheetState,
                upserts: List[Tuple[str,Dict[str,Any]]],
                deletes: List[str]):

    # 0) 배치 중복 스쿼시 + 최신성 가드(시트에 기록된 ts와 비교)
    latest: Dict[str,Dict[str,Any]] = {}
    for pk, rec in upserts:
        ts = int(rec.get(TS_HEADER) or 0)
        prev = latest.get(pk)
        if not prev or ts >= int(prev.get(TS_HEADER) or 0):
            latest[pk] = rec

    # 최신성 가드: 시트에 이미 있는 ts보다 작은 건 스킵
    guarded: List[Tuple[str,Dict[str,Any]]] = []
    skipped = 0
    for pk, rec in latest.items():
        ts = int(rec.get(TS_HEADER) or 0)
        prev_ts = state.mem_ts.get(pk, -1)
        if ts < prev_ts:
            skipped += 1
            continue
        guarded.append((pk, rec))

    if skipped:
        m_msgs_skip.inc(skipped)

    # 1) 업데이트/신규 분리
    updates, appends = [], []
    for pk, rec in guarded:
        row = state.build_row_values(rec)
        row[state.h2c[PK_HEADER]] = pk
        if pk in state.pk_map:
            r = state.pk_map[pk]
            rng = f"{SHEET_NAME}!{a1(0,r)}:{a1(state.ncols-1,r)}"
            updates.append({"range": rng, "values":[row]})
        else:
            appends.append(row)

    # 2) batch update
    if updates:
        m_api_calls.inc()
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption":"RAW","data":updates}
        ).execute()
        m_updates.inc(len(updates))

    # 3) append(신규)
    if appends:
        rng = f"{SHEET_NAME}!A:{a1_col(state.ncols-1)}"
        m_api_calls.inc()
        sheets.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=rng, valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends}
        ).execute()
        m_appends.inc(len(appends))

    # 4) 삭제(일괄)
    if deletes:
        rows = [state.pk_map[pk] for pk in deletes if pk in state.pk_map]
        rows.sort(reverse=True)
        if rows:
            reqs = [{"deleteDimension":{
                "range":{"sheetId":state.sheet_id,"dimension":"ROWS",
                         "startIndex":r-1,"endIndex":r}
            }} for r in rows]
            m_api_calls.inc()
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID, body={"requests": reqs}
            ).execute()
            m_deletes.inc(len(rows))

    # 5) 인덱스/TS 캐시 리빌드(배치 끝에 한 번)
    state.pk_map, state.sheet_ts = load_pk_and_ts(state.h2c[PK_HEADER], state.h2c.get(TS_HEADER))
    # mem_ts 업데이트(성공한 것만 반영)
    for pk, rec in guarded:
        ts = int(rec.get(TS_HEADER) or 0)
        if ts >= state.mem_ts.get(pk, -1):
            state.mem_ts[pk] = ts
    # 삭제된 키 제거
    for pk in deletes:
        state.mem_ts.pop(pk, None)

# ========== Utils ==========
def parse_value(v: Optional[bytes]) -> Dict[str,Any]:
    if not v:
        return {}
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return {}

stop_flag = False
def handle_sigterm(signum, frame):
    global stop_flag
    stop_flag = True

# ========== Main ==========
def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    start_http_server(METRICS_PORT)
    state = SheetState()

    consumer = build_consumer()
    consumer.subscribe([TOPIC])

    try:
        while not stop_flag:
            msgs = consumer.consume(BATCH_SIZE, timeout=POLL_MS/1000.0)
            if not msgs:
                continue

            upserts: List[Tuple[str,Dict[str,Any]]] = []
            deletes: List[str] = []
            last_offsets: List[TopicPartition] = []
            t0 = time.time()

            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        m_msgs_fail.inc()
                    continue
                m_msgs_in.inc()

                val = parse_value(msg.value())
                if not val:
                    m_msgs_skip.inc()
                    continue

                op = (val.get("__op") or val.get("op") or "").lower()
                pk = str(val.get("id") or val.get(PK_HEADER) or "")
                if not pk:
                    m_msgs_skip.inc()
                    if producer: send_dlq(msg, "missing_pk")
                    continue

                # 스냅샷/생성/수정/삭제 분기
                if op in ("d",):
                    deletes.append(pk)
                else:
                    # 시트 최신성 가드를 위해 TS 필드가 없으면 0 처리
                    if TS_HEADER not in val:
                        val[TS_HEADER] = val.get("ts_ms") or 0
                    # 업서트 후보
                    upserts.append((pk, val))

                last_offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset()+1))

            if not (upserts or deletes):
                continue

            try:
                with m_batch_latency.time():
                    apply_batch(state, upserts, deletes)
                # 성공 시 오프셋 커밋(멱등성 보장)
                consumer.commit(offsets=last_offsets, asynchronous=False)
                m_msgs_ok.inc(len(upserts)+len(deletes))
            except (HttpError, ConnectionError, TimeoutError, BrokenPipeError) as e:
                # 재시도 모두 실패
                for msg in msgs:
                    send_dlq(msg, f"http_or_network_error:{type(e).__name__}")
                m_msgs_fail.inc(len(msgs))
            except Exception as e:
                for msg in msgs:
                    send_dlq(msg, f"unexpected:{type(e).__name__}")
                m_msgs_fail.inc(len(msgs))
    finally:
        try:
            consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()

