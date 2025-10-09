import os, time, json, threading
from typing import Dict, List, Any, Tuple
from dateutil import parser as dtparser
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, Producer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- Env ----------
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
SHEET_ID = os.environ["SHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "")  # optional

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
POLL_MS = int(os.environ.get("POLL_MS", "1000"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

# ---------- Metrics ----------
m_msgs_in = Counter("cdc_msgs_in_total", "Messages polled from Kafka")
m_msgs_ok = Counter("cdc_msgs_ok_total", "Messages applied to Sheets successfully")
m_msgs_fail = Counter("cdc_msgs_fail_total", "Messages failed to process")
m_batch_latency = Histogram("cdc_batch_seconds", "Batch end-to-end seconds")
m_lag_rows = Gauge("cdc_sheet_rows", "Row count cached from sheet")

# ---------- Google Sheets ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
def build_sheets():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

sheets = build_sheets()

def a1(col_idx: int, row_idx: int) -> str:
    # 0-based index to A1 (col), 1-based for row
    col = ""
    i = col_idx
    while True:
        i, r = divmod(i, 26)
        col = chr(ord('A') + r) + col
        if i == 0:
            break
        i -= 1
    return f"{col}{row_idx}"

def get_headers_and_index() -> Tuple[List[str], Dict[str,int], int]:
    # Read first row (headers)
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!1:1"
    ).execute()
    values = res.get("values", [])
    headers = values[0] if values else []
    header_to_col = {h: idx for idx, h in enumerate(headers)}
    return headers, header_to_col, len(headers)

def ensure_pk_hidden(pk_col_idx: int):
    # Hide the __pk column if visible
    try:
        body = {
            "requests": [{
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": 0,            # first sheet; adapt if needed
                        "dimension": "COLUMNS",
                        "startIndex": pk_col_idx,
                        "endIndex": pk_col_idx + 1
                    },
                    "properties": { "hiddenByUser": True },
                    "fields": "hiddenByUser"
                }
            }]
        }
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body=body
        ).execute()
    except HttpError as e:
        # not fatal
        print(f"[warn] hide pk column failed: {e}")

def load_pk_map(pk_col_idx: int) -> Dict[str,int]:
    # Read __pk column (from row 2 to end)
    col_a1_start = a1(pk_col_idx, 2)
    col_letter = col_a1_start.rstrip("0123456789")
    rng = f"{SHEET_NAME}!{col_letter}2:{col_letter}"
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=rng, majorDimension="COLUMNS"
    ).execute()
    col = res.get("values", [[]])
    pks = col[0] if col else []
    pk_map = {}
    for i, pk in enumerate(pks, start=2):  # row index (1-based)
        if pk:
            pk_map[pk] = i
    m_lag_rows.set(len(pk_map))
    return pk_map

# ---------- Kafka ----------
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

producer = None
if DLQ_TOPIC:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

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
        print(f"[dlq-fail] {e}")

# ---------- Apply batch to Sheets ----------
class SheetState:
    def __init__(self):
        headers, self.h2c, self.ncols = get_headers_and_index()
        if "__pk" not in self.h2c:
            raise RuntimeError("Sheet must have '__pk' header.")
        ensure_pk_hidden(self.h2c["__pk"])
        self.pk_map = load_pk_map(self.h2c["__pk"])
        self.headers = headers

    def build_row_values(self, record: Dict[str,Any]) -> List[str]:
        row = [""] * self.ncols
        for h, col in self.h2c.items():
            if h == "__pk":
                # fill below
                continue
            if h in record:
                v = record[h]
            else:
                # allow mapping like db 'id'->sheet 'id'
                v = record.get(h)
            if v is None:
                v = ""
            row[col] = str(v)
        return row

@retry(wait=wait_exponential(multiplier=1, min=1, max=30),
       stop=stop_after_attempt(6),
       retry=retry_if_exception_type(HttpError))
def apply_batch(state: SheetState, upserts: List[Tuple[str,Dict[str,Any]]], deletes: List[str]):
    # 1) compute updates & appends
    data_updates = []   # list of {"range": A1, "values":[[...]]}
    appends = []        # values for append
    for pk, rec in upserts:
        row = state.build_row_values(rec)
        row[state.h2c["__pk"]] = pk
        if pk in state.pk_map:
            row_idx = state.pk_map[pk]
            rng = f"{SHEET_NAME}!{a1(0,row_idx)}:{a1(state.ncols-1,row_idx)}"
            data_updates.append({"range": rng, "values":[row]})
        else:
            appends.append(row)

    # 2) batch update existing rows
    if data_updates:
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption":"RAW", "data": data_updates}
        ).execute()

    # 3) append new rows
    if appends:
        res = sheets.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:{a1(state.ncols-1,1).rstrip('1234567890')}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends}
        ).execute()
        # update pk_map for appended rows: need to refetch last row or compute from updates
        # simplest: reload pk_map (cheap enough for small sheets)
        state.pk_map = load_pk_map(state.h2c["__pk"])

    # 4) deletes: soft delete(예시) — __deleted = true 체크 컬럼을 쓰거나, 행 제거도 가능
    # 여기서는 행 삭제 예시 (주의: 큰 시트는 비용 큼)
    for pk in deletes:
        if pk not in state.pk_map:
            continue
        row_idx = state.pk_map[pk]
        req = {
            "requests":[
                {"deleteDimension":{
                    "range":{
                        "sheetId":0,
                        "dimension":"ROWS",
                        "startIndex": row_idx-1,
                        "endIndex": row_idx
                    }
                }}
            ]
        }
        sheets.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body=req).execute()
        # pk_map 재빌드(간단)
        state.pk_map = load_pk_map(state.h2c["__pk"])

def parse_value(v: bytes) -> Dict[str,Any]:
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return {}

def main():
    # metrics
    start_http_server(METRICS_PORT)

    state = SheetState()
    consumer = build_consumer()
    consumer.subscribe([TOPIC])

    while True:
        msgs = consumer.consume(BATCH_SIZE, timeout=POLL_MS/1000.0)
        if not msgs:
            continue

        upserts, deletes, last_offsets = [], [], []
        t0 = time.time()

        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    m_msgs_fail.inc()
                continue
            m_msgs_in.inc()

            val = parse_value(msg.value() or b"{}")
            if not val:
                continue

            op = val.get("__op") or val.get("op")
            # pk 추출: 일반적으로 pk = 'id' (예시)
            pk = str(val.get("id") or val.get("__pk") or "")
            if not pk:
                send_dlq(msg, "missing_pk")
                m_msgs_fail.inc()
                continue

            if op in ("d","D"):
                deletes.append(pk)
            else:
                # 업서트 레코드
                upserts.append((pk, val))

            last_offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset()+1))

        if not (upserts or deletes):
            continue

        try:
            with m_batch_latency.time():
                apply_batch(state, upserts, deletes)
            # 성공 시 오프셋 커밋
            consumer.commit(offsets=last_offsets, asynchronous=False)
            m_msgs_ok.inc(len(upserts)+len(deletes))
        except HttpError as e:
            # 재시도 실패까지 왔으면 DLQ
            reason = f"http_error:{e.status_code if hasattr(e,'status_code') else 'unknown'}"
            for msg in msgs:
                send_dlq(msg, reason)
            m_msgs_fail.inc(len(msgs))
        except Exception as e:
            for msg in msgs:
                send_dlq(msg, f"unexpected:{type(e).__name__}")
            m_msgs_fail.inc(len(msgs))

if __name__ == "__main__":
    main()

