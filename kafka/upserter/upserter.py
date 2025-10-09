#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, signal, json, re
from typing import Dict, List, Any, Tuple, Optional
from datetime import date, timedelta

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================== ENV ==================
KAFKA_GROUP_ID    = os.environ.get("KAFKA_GROUP_ID", "sheets-upserter")
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")
BOOTSTRAP         = os.environ["KAFKA_BOOTSTRAP"]
TOPIC             = os.environ["KAFKA_TOPIC"]
SHEET_ID          = os.environ["SHEET_ID"]
SHEET_NAME        = os.environ.get("SHEET_NAME", "Sheet1")
DLQ_TOPIC         = os.environ.get("DLQ_TOPIC", "")

BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "200"))
POLL_MS      = int(os.environ.get("POLL_MS", "1000"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
DEBUG        = os.environ.get("DEBUG", "1") not in ("0","false","False")

# 동작 플래그
APPEND_ONLY          = os.environ.get("APPEND_ONLY", "0") in ("1","true","True")      # 1이면 append-only (업데이트/삭제 비활성)
ALLOW_ADD_HIDDEN_PK  = os.environ.get("ALLOW_ADD_HIDDEN_PK", "1") in ("1","true","True")
HIDE_PK_COLUMN       = os.environ.get("HIDE_PK_COLUMN", "1") in ("1","true","True")
SORT_BY_PK_ON_START  = os.environ.get("SORT_BY_PK_ON_START", "1") in ("1","true","True")
SORT_BEFORE_APPLY    = os.environ.get("SORT_BEFORE_APPLY", "0") in ("1","true","True")

# CDC 필드명
PK_JSON_KEY      = os.environ.get("PK_JSON_KEY", "id")
OP_JSON_KEY      = os.environ.get("OP_JSON_KEY", "__op")
DELETED_JSON_KEY = os.environ.get("DELETED_JSON_KEY", "__deleted")

# 날짜 변환 설정
DATE_FORMAT       = os.environ.get("DATE_FORMAT", "%Y-%m-%d")
DATE_EPOCH_BASE   = os.environ.get("DATE_EPOCH_BASE", "1970-01-01")  # 일수 기준 시작일
DATE_KEYS         = {"initial_install_date", "failure_date"}         # 변환 대상 키

# A~L까지만 “데이터 유무 판단”에 사용
DATA_LAST_COL_IDX_0 = 12  # 0-indexed; 11 => L열

# ================== Metrics ==================
m_msgs_in       = Counter("cdc_msgs_in_total",          "Messages polled")
m_msgs_ok       = Counter("cdc_msgs_ok_total",          "Messages applied")
m_msgs_fail     = Counter("cdc_msgs_fail_total",        "Messages failed")
m_msgs_skip     = Counter("cdc_msgs_skipped_total",     "Messages skipped")
m_updates       = Counter("cdc_sheet_updates_total",    "Rows updated")
m_appends       = Counter("cdc_sheet_appends_total",    "Rows appended")
m_deletes       = Counter("cdc_sheet_deletes_total",    "Rows deleted")
m_api_calls     = Counter("cdc_sheets_api_calls_total", "Google Sheets API calls")
m_batch_latency = Histogram("cdc_batch_seconds",        "Batch seconds")
m_rows_cached   = Gauge("cdc_sheet_rows",               "Rows cached (pk-indexed)")

# ================== Sheets ==================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def build_sheets():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

sheets = build_sheets()

def a1_col(i0:int)->str:
    col=""; i=i0
    while True:
        i,r=divmod(i,26)
        col=chr(ord('A')+r)+col
        if i==0: break
        i-=1
    return col

def a1(c0:int, r1:int)->str:
    return f"{a1_col(c0)}{r1}"

def get_sheet_id(spreadsheet_id: str, title: str) -> int:
    m_api_calls.inc()
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == title:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"sheet '{title}' not found")

# ====== 날짜 변환 유틸 ======
def _parse_epoch_base(dstr: str) -> date:
    y,m,dd = map(int, dstr.split("-", 2))
    return date(y,m,dd)

_EPOCH_BASE = _parse_epoch_base(DATE_EPOCH_BASE)

def is_int_like(x: Any) -> bool:
    if isinstance(x, int):
        return True
    if isinstance(x, str) and re.fullmatch(r"\d+", x or ""):
        return True
    return False

def convert_date_value(v: Any) -> str:
    """20366 같은 정수는 epoch-days로 보고 YYYY-MM-DD로 변환. 나머지는 그대로(빈값은 빈칸)."""
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
            y,m,d = v.split("/")
            return f"{y}-{m}-{d}"
    return str(v)

# ====== 헤더/표 검사: A1~L1만 ======
def get_headers()->List[str]:
    """1행 헤더 유무는 A1:L1만 체크(사용자 요청)."""
    m_api_calls.inc()
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A1:{a1_col(DATA_LAST_COL_IDX_0)}1"
    ).execute()
    values = res.get("values", [])
    if not values: return []
    row1 = values[0]
    if not row1 or all((str(x).strip()=="" for x in row1)):
        return []
    return row1

def hide_column(sheet_id_int: int, col_idx_0: int):
    try:
        m_api_calls.inc()
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests":[{
                "updateDimensionProperties":{
                    "range":{"sheetId":sheet_id_int,"dimension":"COLUMNS",
                             "startIndex":col_idx_0,"endIndex":col_idx_0+1},
                    "properties":{"hiddenByUser": True},
                    "fields":"hiddenByUser"
                }
            }]}
        ).execute()
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
    """A1이 '__pk'가 되도록 보정(허용 시). 헤더 없으면 A1='__pk'만 기록."""
    headers = get_headers()

    if not headers:
        if not ALLOW_ADD_HIDDEN_PK:
            if DEBUG: print("[init] header empty; __pk not added (ALLOW_ADD_HIDDEN_PK=0)")
            return
        try:
            m_api_calls.inc()
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:A1",
                valueInputOption="RAW",
                body={"values":[["__pk"]]}
            ).execute()
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
        # A열 삽입
        m_api_calls.inc()
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests":[{
                "insertDimension":{
                    "range":{"sheetId": sheet_id_int, "dimension":"COLUMNS",
                             "startIndex":0, "endIndex":1}
                }
            }]}
        ).execute()
        # A1에 __pk
        m_api_calls.inc()
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:A1",
            valueInputOption="RAW",
            body={"values":[["__pk"]]}
        ).execute()
        ensure_pk_hidden_if_needed(sheet_id_int)
        if DEBUG: print("[init] inserted column A and set __pk")
    except HttpError as e:
        if DEBUG: print(f"[warn] ensure_leftmost_pk(insert col): {e}")

def sort_sheet_by_pk(sheet_id_int: int, has_header: bool):
    """A열(__pk) 기준 정렬(헤더 제외) — 선택 사항."""
    try:
        m_api_calls.inc()
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests":[{
                "sortRange":{
                    "range":{
                        "sheetId": sheet_id_int,
                        "startRowIndex": 1 if has_header else 0,
                        "startColumnIndex": 0,
                    },
                    "sortSpecs":[{"dimensionIndex": 0, "sortOrder": "ASCENDING"}]
                }
            }]}
        ).execute()
        if DEBUG: print("[init] sorted entire sheet by __pk (A column)")
    except HttpError as e:
        if DEBUG: print(f"[warn] sort_sheet_by_pk: {e}")

# ====== A~L만 보고 "다음에 쓸 행" 찾기 ======
def find_next_row_for_AL(start_row: int) -> int:
    """
    A{start_row}:L 범위를 읽고, A~L 중 하나라도 값이 있는 **마지막 행 바로 아래**를 반환.
    M열 이후에 사용자가 뭘 적어놔도 **무시**.
    """
    last_col_letter = a1_col(DATA_LAST_COL_IDX_0)  # 'L'
    m_api_calls.inc()
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A{start_row}:{last_col_letter}",
        majorDimension="ROWS"
    ).execute()
    rows = res.get("values", [])
    last = start_row - 1
    for idx, row in enumerate(rows, start=start_row):
        # row 길이가 짧아도 빈칸으로 간주됨
        if any((str(cell).strip() != "" for cell in row)):
            last = idx
    return last + 1  # 다음 행

# ================== Kafka ==================
def build_consumer()->Consumer:
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

producer = Producer({"bootstrap.servers": BOOTSTRAP}) if DLQ_TOPIC else None

def send_dlq(msg, reason:str):
    if not producer: return
    try:
        payload = {
            "reason": reason,
            "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8") if msg.value() else None,
            "timestamp": msg.timestamp()[1],
        }
        producer.produce(DLQ_TOPIC, json.dumps(payload).encode("utf-8"))
        producer.flush(3)
    except Exception as e:
        print(f"[dlq-fail] {e}", file=sys.stderr)

# ================== State ==================
class SheetState:
    def __init__(self):
        self.sheet_id_int = get_sheet_id(SHEET_ID, SHEET_NAME)

        # __pk 보장/숨김
        ensure_leftmost_pk(self.sheet_id_int)

        # 헤더/시작행 계산
        self.headers = get_headers()
        self.header_mode = len(self.headers) > 0
        self.start_row = 2 if self.header_mode else 1
        self.has_pk_col = bool(self.header_mode and self.headers and _is_pk_header(self.headers[0]))

        if self.has_pk_col and SORT_BY_PK_ON_START:
            sort_sheet_by_pk(self.sheet_id_int, self.header_mode)

        # pk → 다중 행 목록 (업데이트/삭제용)
        self.pk_rows: Dict[str, List[int]] = (
            self._load_pk_rows_from_A() if (self.has_pk_col and not APPEND_ONLY) else {}
        )
        m_rows_cached.set(sum(len(v) for v in self.pk_rows.values()))

        if DEBUG:
            print(f"[flags] APPEND_ONLY={APPEND_ONLY} ALLOW_ADD_HIDDEN_PK={ALLOW_ADD_HIDDEN_PK} HIDE_PK_COLUMN={HIDE_PK_COLUMN} SORT_BY_PK_ON_START={SORT_BY_PK_ON_START} SORT_BEFORE_APPLY={SORT_BEFORE_APPLY}")
            print(f"[init] header_mode={self.header_mode} start_row={self.start_row} has_pk_col={self.has_pk_col}")

    def _load_pk_rows_from_A(self) -> Dict[str, List[int]]:
        m_api_calls.inc()
        res = sheets.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A{self.start_row}:A",
            majorDimension="COLUMNS"
        ).execute()
        vals = (res.get("values", [[]]) or [[]])[0]
        pk_rows: Dict[str, List[int]] = {}
        for i, v in enumerate(vals, start=self.start_row):
            if v is None or str(v) == "":
                continue
            k = str(v)
            pk_rows.setdefault(k, []).append(i)
        return pk_rows

    def build_row_values(self, record: Dict[str,Any], pk: str) -> List[str]:
        """CDC JSON 순서 그대로, 날짜 키는 사람이 읽게 변환."""
        out: List[str] = []
        for k, v in record.items():
            if k in DATE_KEYS:
                out.append(convert_date_value(v))
            else:
                out.append("" if v is None else str(v))
        row = ([pk or ""] + out) if self.has_pk_col else out
        return row

# ================== Apply ==================
@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(6),
       retry=retry_if_exception_type((HttpError, ConnectionError, TimeoutError, BrokenPipeError)))
def apply_batch(state: SheetState,
                upserts: List[Tuple[str,Dict[str,Any]]],
                deletes: List[str]):

    if state.has_pk_col and SORT_BEFORE_APPLY:
        sort_sheet_by_pk(state.sheet_id_int, state.header_mode)

    if state.has_pk_col and not APPEND_ONLY:
        state.pk_rows = state._load_pk_rows_from_A()
        m_rows_cached.set(sum(len(v) for v in state.pk_rows.values()))

    updates, appends = [], []

    for pk, rec in upserts:
        row = state.build_row_values(rec, pk)
        if state.has_pk_col and not APPEND_ONLY and pk and pk in state.pk_rows:
            # 동일 pk가 여러개면 맨 아래 것 업데이트
            r = state.pk_rows[pk][-1]
            last_col_index = max(0, len(row) - 1)
            rng = f"{SHEET_NAME}!A{r}:{a1_col(last_col_index)}{r}"
            updates.append({"range": rng, "values":[row]})
        else:
            appends.append(row)

    # 업데이트 먼저
    if updates:
        m_api_calls.inc()
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption":"RAW","data":updates}
        ).execute()
        m_updates.inc(len(updates))
        if DEBUG: print(f"[apply] updated={len(updates)}")

    # ★ Append: A~L만 보고 “다음에 쓸 행”을 계산해서 그 위치에 바로 update로 기록
    if appends:
        next_row = find_next_row_for_AL(state.start_row)
        # 직사각형으로 쓰기 위해 길이 패딩
        max_len = max(len(r) for r in appends)
        padded = [r + [""]*(max_len - len(r)) for r in appends]
        end_row = next_row + len(padded) - 1
        rng = f"{SHEET_NAME}!A{next_row}:{a1_col(max_len-1)}{end_row}"
        m_api_calls.inc()
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=rng,
            valueInputOption="RAW",
            body={"values": padded}
        ).execute()
        m_appends.inc(len(appends))
        if DEBUG: print(f"[apply] appended={len(appends)} at rows {next_row}..{end_row} (A..{a1_col(max_len-1)})")

    # 삭제: pk 기준으로 모든 행 제거(옵션 off면 무시)
    if deletes and state.has_pk_col and not APPEND_ONLY:
        # 적용 직전 최신 맵으로
        state.pk_rows = state._load_pk_rows_from_A()
        del_rows: List[int] = []
        for pk in deletes:
            rows = state.pk_rows.get(pk, [])
            if rows:
                del_rows.extend(rows)
        del_rows = sorted(set(del_rows), reverse=True)
        if del_rows:
            reqs = [{"deleteDimension":{
                "range":{"sheetId": state.sheet_id_int,
                         "dimension":"ROWS",
                         "startIndex": r-1, "endIndex": r}
            }} for r in del_rows]
            m_api_calls.inc()
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID, body={"requests": reqs}
            ).execute()
            m_deletes.inc(len(del_rows))
            if DEBUG: print(f"[apply] deleted rows={del_rows}")

    if state.has_pk_col and not APPEND_ONLY:
        state.pk_rows = state._load_pk_rows_from_A()
        m_rows_cached.set(sum(len(v) for v in state.pk_rows.values()))

# ================== Utils/Main ==================
def parse_json(b: Optional[bytes])->Dict[str,Any]:
    if not b: return {}
    try: return json.loads(b.decode("utf-8"))
    except Exception:
        return {}

stop_flag=False
def handle_sigterm(signum, frame):
    global stop_flag; stop_flag=True

def build_consumer()->Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
        "allow.auto.create.topics": False,
    })

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
            if not msgs: continue

            upserts: List[Tuple[str,Dict[str,Any]]] = []
            deletes: List[str] = []
            last_offsets: List[TopicPartition] = []

            for msg in msgs:
                if msg.error():
                    if msg.error().code()!=KafkaError._PARTITION_EOF:
                        m_msgs_fail.inc()
                        if DEBUG: print(f"[kafka-err] {msg.error()}")
                    continue

                m_msgs_in.inc()
                val = parse_json(msg.value())
                key = parse_json(msg.key())

                # delete 판정
                op = (val.get(OP_JSON_KEY) or val.get("op") or "").lower() if val else ""
                deleted_flag = (str(val.get(DELETED_JSON_KEY,"")).lower()=="true") if val else False
                tombstone_delete = (not val) and key and (PK_JSON_KEY in key and key[PK_JSON_KEY] is not None)
                is_delete = (op=="d") or deleted_flag or tombstone_delete

                # pk 추출 (value 우선, 없으면 key에서)
                pk = None
                for src in (val, key):
                    if not src: continue
                    if PK_JSON_KEY in src and src[PK_JSON_KEY] is not None:
                        pk = str(src[PK_JSON_KEY]); break

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

                last_offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset()+1))

            if not (upserts or deletes): continue

            try:
                with m_batch_latency.time():
                    apply_batch(state, upserts, deletes)
                consumer.commit(offsets=last_offsets, asynchronous=False)
                m_msgs_ok.inc(len(upserts)+len(deletes))
            except (HttpError, ConnectionError, TimeoutError, BrokenPipeError) as e:
                if DEBUG: print(f"[http/network] {type(e).__name__}: {e}")
                m_msgs_fail.inc(len(msgs))
            except Exception as e:
                if DEBUG: print(f"[unexpected] {type(e).__name__}: {e}")
                m_msgs_fail.inc(len(msgs))
    finally:
        try: consumer.close()
        except Exception: pass

if __name__ == "__main__":
    main()

