#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, signal, json, re
from typing import Dict, List, Any, Tuple, Optional
from datetime import date, timedelta
from collections import OrderedDict

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================== ENV ==================
NOTIFY_TOPIC = os.environ.get("NOTIFY_TOPIC", "notify.rma")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "sheets-upserter")
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
SHEET_ID = os.environ["SHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
POLL_MS = int(os.environ.get("POLL_MS", "1000"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
DEBUG = os.environ.get("DEBUG", "1") not in ("0", "false", "False")

# 동작 플래그
APPEND_ONLY = os.environ.get("APPEND_ONLY", "0") in ("1", "true", "True")
ALLOW_ADD_HIDDEN_PK = os.environ.get("ALLOW_ADD_HIDDEN_PK", "1") in ("1", "true", "True")
HIDE_PK_COLUMN = os.environ.get("HIDE_PK_COLUMN", "1") in ("1", "true", "True")
SORT_BY_PK_ON_START = os.environ.get("SORT_BY_PK_ON_START", "1") in ("1", "true", "True")
SORT_BEFORE_APPLY = os.environ.get("SORT_BEFORE_APPLY", "0") in ("1", "true", "True")

# CDC 필드명
PK_JSON_KEY = os.environ.get("PK_JSON_KEY", "id")
OP_JSON_KEY = os.environ.get("OP_JSON_KEY", "__op")
DELETED_JSON_KEY = os.environ.get("DELETED_JSON_KEY", "__deleted")

# 날짜 변환 설정
DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y-%m-%d")
DATE_EPOCH_BASE = os.environ.get("DATE_EPOCH_BASE", "1970-01-01")
DATE_KEYS = {"initial_install_date", "failure_date"}
DATE_CONVERT_ON = os.environ.get("DATE_CONVERT_ON", "1") in ("1", "true", "True")

# 데이터 유무 판단 마지막 컬럼(A..L/M 등) — 0-index (L=11, M=12)
DATA_LAST_COL_IDX_0 = int(os.environ.get("DATA_LAST_COL_IDX_0", "13"))

# LRU 캐시 설정
PK_CACHE_MAX_KEYS = int(os.environ.get("PK_CACHE_MAX_KEYS", "1000"))

# ================== Metrics ==================
m_msgs_in = Counter("cdc_msgs_in_total", "Messages polled")
m_msgs_ok = Counter("cdc_msgs_ok_total", "Messages applied")
m_msgs_fail = Counter("cdc_msgs_fail_total", "Messages failed")
m_msgs_skip = Counter("cdc_msgs_skipped_total", "Messages skipped")
m_updates = Counter("cdc_sheet_updates_total", "Rows updated")
m_appends = Counter("cdc_sheet_appends_total", "Rows appended")
m_deletes = Counter("cdc_sheet_deletes_total", "Rows deleted")
m_api_calls = Counter("cdc_sheets_api_calls_total", "Google Sheets API calls")
m_batch_latency = Histogram("cdc_batch_seconds", "Batch seconds")
m_rows_cached = Gauge("cdc_sheet_rows", "Rows indexed in cache")

# ================== Sheets ==================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def build_sheets():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


sheets = build_sheets()


# --- 공통 실행 래퍼: 403/401 등 HttpError 상태/본문 로깅 ---
def exec_or_log(req, where: str):
    try:
        return req.execute()
    except HttpError as e:
        status = getattr(getattr(e, "resp", None), "status", "?")
        body = ""
        try:
            raw = getattr(e, "content", b"")
            body = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
        except Exception:
            body = "<no-body>"
        print(f"[sheets][{where}] HttpError status={status} body_sample={body[:500]!r}")
        raise


def a1_col(i0: int) -> str:
    col = "";
    i = i0
    while True:
        i, r = divmod(i, 26)
        col = chr(ord('A') + r) + col
        if i == 0: break
        i -= 1
    return col


def a1(c0: int, r1: int) -> str:
    return f"{a1_col(c0)}{r1}"


def get_sheet_id(spreadsheet_id: str, title: str) -> int:
    m_api_calls.inc()
    meta = exec_or_log(
        sheets.spreadsheets().get(spreadsheetId=spreadsheet_id),
        "spreadsheets.get(meta)"
    )
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == title:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"sheet '{title}' not found")


# ====== 날짜 변환 유틸 ======
def _parse_epoch_base(dstr: str) -> date:
    y, m, dd = map(int, dstr.split("-", 2))
    return date(y, m, dd)


_EPOCH_BASE = _parse_epoch_base(DATE_EPOCH_BASE)


def is_int_like(x: Any) -> bool:
    if isinstance(x, int):
        return True
    if isinstance(x, str) and re.fullmatch(r"\d+", x or ""):
        return True
    return False


def convert_date_value(v: Any) -> str:
    if not DATE_CONVERT_ON:
        return "" if v is None else str(v)
    if v is None or v == "":
        return ""
    if is_int_like(v):
        days = int(v)
        dt = _EPOCH_BASE + timedelta(days=days)
        return dt.strftime(DATE_FORMAT)
    if isinstance(v, str):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            return v
        if re.fullmatch(r"\d{4}/\d{2}/\d{2}", v):
            y, m, d = v.split("/")
            return f"{y}-{m}-{d}"
    return str(v)


# ====== 헤더: A1..(마지막)만 체크 ======
def get_headers() -> List[str]:
    m_api_calls.inc()
    res = exec_or_log(
        sheets.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A1:{a1_col(DATA_LAST_COL_IDX_0)}1"
        ),
        "values.get(headers)"
    )
    values = res.get("values", [])
    if not values: return []
    row1 = values[0]
    if not row1 or all((str(x).strip() == "" for x in row1)):
        return []
    return row1


def hide_column(sheet_id_int: int, col_idx_0: int):
    try:
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id_int, "dimension": "COLUMNS",
                                  "startIndex": col_idx_0, "endIndex": col_idx_0 + 1},
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser"
                    }
                }]}
            ),
            "batchUpdate.hide_column"
        )
    except HttpError as e:
        if DEBUG: print(f"[warn] hide_column: {e}")


def ensure_pk_hidden_if_needed(sheet_id_int: int):
    if not HIDE_PK_COLUMN: return
    try:
        hide_column(sheet_id_int, 0)  # A열
        if DEBUG: print("[init] ensured A column is hidden")
    except Exception as e:
        if DEBUG: print(f"[warn] ensure_pk_hidden_if_needed: {e}")


def _is_pk_header(cell: Optional[str]) -> bool:
    return isinstance(cell, str) and cell.strip().lower() == "__pk"


def ensure_leftmost_pk(sheet_id_int: int):
    headers = get_headers()
    if not headers:
        if not ALLOW_ADD_HIDDEN_PK:
            if DEBUG: print("[init] header empty; __pk not added (ALLOW_ADD_HIDDEN_PK=0)")
            return
        try:
            m_api_calls.inc()
            exec_or_log(
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID,
                    range=f"{SHEET_NAME}!A1:A1",
                    valueInputOption="RAW",
                    body={"values": [["__pk"]]}
                ),
                "values.update(A1=__pk)"
            )
            ensure_pk_hidden_if_needed(sheet_id_int)
            if DEBUG: print("[init] set __pk at A1 on empty header")
        except HttpError as e:
            if DEBUG: print(f"[warn] ensure_leftmost_pk(empty): {e}")
        return

    if _is_pk_header(headers[0]):
        ensure_pk_hidden_if_needed(sheet_id_int)
        return

    if not ALLOW_ADD_HIDDEN_PK:
        if DEBUG: print("[init] __pk not added (ALLOW_ADD_HIDDEN_PK=0)")
        return

    try:
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{
                    "insertDimension": {
                        "range": {"sheetId": sheet_id_int, "dimension": "COLUMNS",
                                  "startIndex": 0, "endIndex": 1}
                    }
                }]}
            ),
            "batchUpdate.insertDimension(A)"
        )
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:A1",
                valueInputOption="RAW",
                body={"values": [["__pk"]]}
            ),
            "values.update(A1=__pk,after-insert)"
        )
        ensure_pk_hidden_if_needed(sheet_id_int)
        if DEBUG: print("[init] inserted column A and set __pk")
    except HttpError as e:
        if DEBUG: print(f"[warn] ensure_leftmost_pk(insert col): {e}")


def sort_sheet_by_pk(sheet_id_int: int, has_header: bool):
    try:
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{
                    "sortRange": {
                        "range": {
                            "sheetId": sheet_id_int,
                            "startRowIndex": 1 if has_header else 0,
                            "startColumnIndex": 0,
                        },
                        "sortSpecs": [{"dimensionIndex": 0, "sortOrder": "ASCENDING"}]
                    }
                }]}
            ),
            "batchUpdate.sortRange(A)"
        )
        if DEBUG: print("[init] sorted entire sheet by __pk (A column)")
    except HttpError as e:
        if DEBUG: print(f"[warn] sort_sheet_by_pk: {e}")


# ====== A..(마지막)만 보고 “다음 쓸 행” 계산 ======
def find_next_row_for_range(start_row: int) -> int:
    last_col_letter = a1_col(DATA_LAST_COL_IDX_0)
    m_api_calls.inc()
    res = exec_or_log(
        sheets.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A{start_row}:{last_col_letter}",
            majorDimension="ROWS"
        ),
        "values.get(find_next_row)"
    )
    rows = res.get("values", [])
    last = start_row - 1
    for idx, row in enumerate(rows, start=start_row):
        if any((str(cell).strip() != "" for cell in row)):
            last = idx
    return last + 1


# ================== Kafka ==================
def build_consumer() -> Consumer:
    if DEBUG:
        print(f"[kafka] bootstrap={BOOTSTRAP} topic={TOPIC} group={KAFKA_GROUP_ID} reset={AUTO_OFFSET_RESET}")
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
        "allow.auto.create.topics": False,
    })


producer = Producer({"bootstrap.servers": BOOTSTRAP}) if (DLQ_TOPIC or NOTIFY_TOPIC) else None


def _on_delivery(err, msg):
    if err:
        print(f"[delivery-error] {err} topic={msg.topic()} key={msg.key()}", file=sys.stderr)
        # TODO: 여기서 재시도 or DLQ로 복구 로직
    else:
        if DEBUG:
            print(f"[delivered] {msg.topic()} p={msg.partition()} off={msg.offset()} key={msg.key()}")


def send_notify(email: str, ticket_id: str, record: dict):
    if not (producer and NOTIFY_TOPIC):
        if DEBUG: print("[notify-skip] producer or NOTIFY_TOPIC missing")
        return

    tid = (ticket_id or record.get("id") or "").strip()
    to_email = (email or record.get("email") or "").strip()
    if not (tid and to_email):
        if DEBUG: print(f"[notify-skip] missing email or ticket_id: email={to_email} ticket_id={tid}")
        return

    payload = {
        "name": record.get("name"),
        "model": record.get("model"),
        "serial_number": record.get("serial_number"),
        "email": to_email,
        "ticket_id": ticket_id,
        "status": record.get("status"),
        "submitted_at_ms": record.get("__source_ts_ms"),
    }

    while True:  # 전송 에러 시 재시도 루프
        try:
            producer.produce(  ##NOTIFY_TOPIC Partition key 설정
                NOTIFY_TOPIC,
                key=tid.encode("utf-8"),
                value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                on_delivery=_on_delivery            ## callback
            )
            if DEBUG: print(f"[notify-send] email={to_email} ticket_id={tid}")
            print(f"[notify-send] email={to_email} ticket_id={tid}(forced)")
            producer.poll(0.05)
            break
        except BufferError as e:
            # 큐가 찼을 때 drain
            print(f"[Error-DrainBuffer] {e}", file=sys.stderr)
            producer.poll(0.2)

        except Exception as e:
            print(f"[notify-send-fail] {e}", file=sys.stderr)
            producer.poll(0.2)
            break



def send_dlq(msg, reason: str):
    if not producer: return
    try:
        payload = {
            "reason": reason,
            "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8") if msg.value() else None,
            "timestamp": msg.timestamp()[1],
        }
        producer.produce(DLQ_TOPIC, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.flush(3)
    except Exception as e:
        print(f"[dlq-fail] {e}", file=sys.stderr)


# ================== LRU + Lazy Loader ==================
class PKRowIndexLRU:
    def __init__(self, start_row: int, max_keys: int):
        self.start_row = start_row
        self.max_keys = max_keys
        self.snapshot: List[str] = []
        self.cache: "OrderedDict[str, List[int]]" = OrderedDict()

    def refresh_snapshot(self):
        m_api_calls.inc()
        res = exec_or_log(
            sheets.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A{self.start_row}:A",
                majorDimension="COLUMNS"
            ),
            "values.get(snapshot A)"
        )
        self.snapshot = (res.get("values", [[]]) or [[]])[0]
        self.cache.clear()
        m_rows_cached.set(0)
        if DEBUG: print(f"[cache] snapshot refreshed, length={len(self.snapshot)}")

    def _scan_rows_for_pk(self, pk: str) -> List[int]:
        rows: List[int] = []
        if not self.snapshot:
            self.refresh_snapshot()
        for i, v in enumerate(self.snapshot, start=self.start_row):
            if v is not None and str(v) == pk:
                rows.append(i)
        return rows

    def get_rows(self, pk: str) -> List[int]:
        if pk in self.cache:
            rows = self.cache.pop(pk)
            self.cache[pk] = rows
            return rows
        rows = self._scan_rows_for_pk(pk)
        self.cache[pk] = rows
        if len(self.cache) > self.max_keys:
            self.cache.popitem(last=False)
        m_rows_cached.set(sum(len(v) for v in self.cache.values()))
        return rows

    def verify_row_has_pk(self, row: int, pk: str) -> bool:
        m_api_calls.inc()
        res = exec_or_log(
            sheets.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A{row}:A{row}",
                majorDimension="ROWS"
            ),
            f"values.get(verify A{row})"
        )
        vals = res.get("values", [])
        cur = (vals[0][0] if (vals and vals[0]) else "")
        return str(cur) == str(pk)

    def invalidate(self):
        self.refresh_snapshot()


# ================== State ==================
class SheetState:
    def __init__(self):
        self.sheet_id_int = get_sheet_id(SHEET_ID, SHEET_NAME)

        ensure_leftmost_pk(self.sheet_id_int)

        self.headers = get_headers()
        self.header_mode = len(self.headers) > 0
        self.start_row = 2 if self.header_mode else 1
        self.has_pk_col = bool(self.header_mode and self.headers and _is_pk_header(self.headers[0]))

        if self.has_pk_col and SORT_BY_PK_ON_START:
            sort_sheet_by_pk(self.sheet_id_int, self.header_mode)

        self.pk_index = PKRowIndexLRU(self.start_row, PK_CACHE_MAX_KEYS)

        if DEBUG:
            print(
                f"[flags] APPEND_ONLY={APPEND_ONLY} ALLOW_ADD_HIDDEN_PK={ALLOW_ADD_HIDDEN_PK} HIDE_PK_COLUMN={HIDE_PK_COLUMN} SORT_BY_PK_ON_START={SORT_BY_PK_ON_START} SORT_BEFORE_APPLY={SORT_BEFORE_APPLY}")
            print(f"[init] header_mode={self.header_mode} start_row={self.start_row} has_pk_col={self.has_pk_col}")

    def build_row_values(self, record: Dict[str, Any], pk: str) -> List[str]:
        out: List[str] = []
        for k, v in record.items():
            if DATE_CONVERT_ON and (k in DATE_KEYS):
                out.append(convert_date_value(v))
            else:
                out.append("" if v is None else str(v))
        row = ([pk or ""] + out) if self.has_pk_col else out
        return row


# ================== Apply ==================
@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(6),
       retry=retry_if_exception_type((HttpError, ConnectionError, TimeoutError, BrokenPipeError)))
def apply_batch(state: SheetState,
                upserts: List[Tuple[str, Dict[str, Any]]],
                deletes: List[str]):
    if state.has_pk_col and SORT_BEFORE_APPLY:
        sort_sheet_by_pk(state.sheet_id_int, state.header_mode)

    updates, appends = [], []
    print(f"[apply] upserts={len(upserts)} deletes={len(deletes)}")
    print(
        f"[apply] first upsert pk={upserts[0][0] if upserts else '-'} last upsert pk={upserts[-1][0] if upserts else '-'}")

    # 1) UPSERT
    for pk, rec in upserts:
        row_vals = state.build_row_values(rec, pk)
        if state.has_pk_col and not APPEND_ONLY and pk:
            rows = state.pk_index.get_rows(pk)
            if rows:
                target = rows[-1]
                if not state.pk_index.verify_row_has_pk(target, pk):
                    state.pk_index.invalidate()
                    rows = state.pk_index.get_rows(pk)
                    target = rows[-1] if rows else None
                if target:
                    rng = f"{SHEET_NAME}!A{target}:{a1_col(len(row_vals) - 1)}{target}"
                    updates.append({"range": rng, "values": [row_vals]})
                    continue
        appends.append(row_vals)

    # 2) UPDATE
    if updates:
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "RAW", "data": updates}
            ),
            "values.batchUpdate(upserts)"
        )
        m_updates.inc(len(updates))
        if DEBUG: print(f"[apply] updated={len(updates)}")
        state.pk_index.invalidate()

    # 3) APPEND (구멍 없이 직사각형 update)
    if appends:
        next_row = find_next_row_for_range(state.start_row)
        max_len = max(len(r) for r in appends)
        padded = [r + [""] * (max_len - len(r)) for r in appends]
        end_row = next_row + len(padded) - 1
        rng = f"{SHEET_NAME}!A{next_row}:{a1_col(max_len - 1)}{end_row}"
        m_api_calls.inc()
        exec_or_log(
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=rng,
                valueInputOption="RAW",
                body={"values": padded}
            ),
            "values.update(append-rect)"
        )
        m_appends.inc(len(appends))
        if DEBUG: print(f"[apply] appended={len(appends)} at rows {next_row}..{end_row}")
        state.pk_index.invalidate()

    # 4) DELETE
    if deletes and state.has_pk_col and not APPEND_ONLY:
        del_rows: List[int] = []
        for pk in deletes:
            rows = state.pk_index.get_rows(pk)
            if not rows:
                continue
            for r in rows:
                if state.pk_index.verify_row_has_pk(r, pk):
                    del_rows.append(r)
                else:
                    state.pk_index.invalidate()
                    rows2 = state.pk_index.get_rows(pk)
                    if r in rows2 and state.pk_index.verify_row_has_pk(r, pk):
                        del_rows.append(r)
        if del_rows:
            del_rows = sorted(set(del_rows), reverse=True)
            reqs = [{"deleteDimension": {
                "range": {"sheetId": state.sheet_id_int,
                          "dimension": "ROWS",
                          "startIndex": r - 1, "endIndex": r}
            }} for r in del_rows]
            m_api_calls.inc()
            exec_or_log(
                sheets.spreadsheets().batchUpdate(
                    spreadsheetId=SHEET_ID, body={"requests": reqs}
                ),
                "batchUpdate.deleteDimension(rows)"
            )
            m_deletes.inc(len(del_rows))
            if DEBUG: print(f"[apply] deleted rows={del_rows}")
            state.pk_index.invalidate()


# ================== Utils/Main ==================
def parse_json(b: Optional[bytes]) -> Dict[str, Any]:
    if not b: return {}
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {}


stop_flag = False


def handle_sigterm(signum, frame):
    global stop_flag;
    stop_flag = True


def print_assignment(consumer, partitions):
    try:
        print("Assigned Partitions:", [p.partition for p in partitions])
        consumer.assign(partitions)
    except Exception as e:
        print(f"[assign-error] {type(e).__name__}: {e}")


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    start_http_server(METRICS_PORT)

    state = SheetState()
    consumer = build_consumer()
    consumer.subscribe([TOPIC], on_assign=print_assignment)

    try:
        while not stop_flag:
            msgs = consumer.consume(BATCH_SIZE, timeout=POLL_MS / 1000.0)
            if not msgs: continue

            upserts: List[Tuple[str, Dict[str, Any]]] = []
            deletes: List[str] = []
            last_offsets: List[TopicPartition] = []

            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        m_msgs_fail.inc()
                        if DEBUG: print(f"[kafka-err] {msg.error()}")
                    continue
                # Pairtion 할당 조회
                print(f"Consumed from partition {msg.partition()}")

                m_msgs_in.inc()
                val = parse_json(msg.value())
                key = parse_json(msg.key())

                op = (val.get(OP_JSON_KEY) or val.get("op") or "").lower() if val else ""
                deleted_flag = (str(val.get(DELETED_JSON_KEY, "")).lower() == "true") if val else False
                tombstone_delete = (not val) and key and (PK_JSON_KEY in key and key[PK_JSON_KEY] is not None)
                is_delete = (op == "d") or deleted_flag or tombstone_delete

                pk = None
                for src in (val, key):
                    if not src: continue
                    if PK_JSON_KEY in src and src[PK_JSON_KEY] is not None:
                        pk = str(src[PK_JSON_KEY]);
                        break

                if is_delete:
                    if state.has_pk_col and not APPEND_ONLY and pk:
                        deletes.append(pk)
                    else:
                        m_msgs_skip.inc()
                        if DEBUG: print(f"[skip] delete ignored (no pk col or append-only): pk={pk}")
                else:
                    if not val:
                        m_msgs_skip.inc()
                        if DEBUG: print(f"[skip] empty value")
                    else:
                        upserts.append((pk or "", val))

                last_offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1))
                print(
                    f"[debug] partition={msg.partition()} offset={msg.offset()} key={msg.key()} pk={pk} op={op} delete={is_delete}")

            if not (upserts or deletes): continue

            try:
                with m_batch_latency.time():
                    apply_batch(state, upserts, deletes)

                consumer.commit(offsets=last_offsets, asynchronous=False)
                m_msgs_ok.inc(len(upserts) + len(deletes))

                if NOTIFY_TOPIC:
                    for pk, rec in upserts:
                        email = rec.get("email")
                        if email:
                            send_notify(rec["email"], rec.get("id"), rec)
                if producer:
                    try:
                        producer.flush(3)
                    except Exception as e:
                        print(f"[producer.flush-error] {e}", file=sys.stderr)
                        pass

            except HttpError as e:
                status = getattr(getattr(e, "resp", None), "status", "?")
                body = ""
                try:
                    raw = getattr(e, "content", b"")
                    body = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
                except Exception:
                    pass
                if DEBUG: print(f"[main] HttpError status={status} body_sample={body[:500]!r}")
                m_msgs_fail.inc(len(msgs))
            except (ConnectionError, TimeoutError, BrokenPipeError) as e:
                if DEBUG: print(f"[http/network] {type(e).__name__}: {e}")
                m_msgs_fail.inc(len(msgs))
            except Exception as e:
                if DEBUG: print(f"[unexpected] {type(e).__name__}: {e}")
                m_msgs_fail.inc(len(msgs))
    finally:
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

