from confluent_kafka import Consumer
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "당신의-시트-ID"
SHEET_NAME = "customers"  # 탭 이름
HEADER = ["__PK","id","name","email","__op","__deleted","__source_ts_ms"]

def compute_pk(msg):
    # 단일 PK
    return str(msg["id"])
    # 복합이면: return f'{msg["country_code"]}|{msg["customer_no"]}'

def load_index(svc):
    # 시트 읽고 __PK -> rowIndex 매핑 구성 (1-based row, 헤더=1)
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A2:A").execute()
    pk_list = resp.get("values", [])
    index = {}
    for i, row in enumerate(pk_list, start=2):
        if row: index[row[0]] = i
    return index

def upsert_row(svc, idx, pk, row_dict):
    if pk in idx:
        row = idx[pk]
        body = {"values": [[pk, row_dict.get("id"), row_dict.get("name"),
                            row_dict.get("email"), row_dict.get("__op"),
                            row_dict.get("__deleted"), row_dict.get("__source_ts_ms")]]}
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A{row}:G{row}", valueInputOption="RAW", body=body).execute()
    else:
        body = {"values": [[pk, row_dict.get("id"), row_dict.get("name"),
                            row_dict.get("email"), row_dict.get("__op"),
                            row_dict.get("__deleted"), row_dict.get("__source_ts_ms")]]}
        svc.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:A",
            valueInputOption="RAW", insertDataOption="INSERT_ROWS", body=body).execute()

def delete_row(svc, idx, pk):
    if pk not in idx: return
    row = idx[pk]
    # batchUpdate로 행 삭제(1-based index, inclusive)
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{"deleteDimension": {
            "range": {"sheetId": SHEET_TAB_ID, "dimension": "ROWS", "startIndex": row-1, "endIndex": row}
        }}]}).execute()
    # 인덱스 재계산 필요(간단히 전체 리빌드)

# --- 구글 인증/서비스 ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service-account.json", scopes=SCOPES)
svc = build("sheets", "v4", credentials=creds).spreadsheets()

# 헤더 준비(최초 1회)
# svc.values().update(... range=f"{SHEET_NAME}!A1:G1", body={"values":[HEADER]}, valueInputOption="RAW").execute()

index = load_index(svc)

# --- Kafka ---
c = Consumer({"bootstrap.servers":"doa-kafka-kp-brokers:9092","group.id":"sheets-updater","auto.offset.reset":"earliest"})
c.subscribe(["cdc.public.customers"])

while True:
    m = c.poll(1.0)
    if not m or m.error(): continue
    val = json.loads(m.value().decode())
    pk = compute_pk(val)
    if val.get("__deleted") == "true" or val.get("__op") == "d":
        delete_row(svc, index, pk)
        # index 재빌드(간단) or 삭제 이후 인덱스 보정
        index = load_index(svc)
    else:
        upsert_row(svc, index, pk, val)
        # 새로 추가됐다면 인덱스 갱신
        index = load_index(svc)

