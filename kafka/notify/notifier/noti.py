#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, signal, json, time, traceback, random
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from email import policy
from email.message import EmailMessage
import smtplib

from confluent_kafka import Consumer, KafkaException, KafkaError, Producer, TopicPartition

# ================== ENV ==================
KAFKA_BOOTSTRAP   = os.environ["KAFKA_BOOTSTRAP"]  # 반드시 FQDN:PORT (예: <cluster>-kafka-bootstrap.ns.svc.cluster.local:9092)
NOTIFY_TOPIC      = os.environ.get("NOTIFY_TOPIC", "notify.rma")
NOTI_GROUP_ID     = os.environ.get("NOTI_GROUP_ID", "notifier-rma")

# (선택) 실패시 DLQ로 내보내기
DLQ_TOPIC         = os.environ.get("DLQ_TOPIC", "")  # 비우면 DLQ 미사용

SMTP_HOST         = os.environ.get("SMTP_HOST", "")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASS         = os.environ.get("SMTP_PASS", "")
MAIL_FROM         = os.environ.get("MAIL_FROM", "")
LOGO_PATH         = os.environ.get("LOGO_PATH", "/app/assets/logo.png")

DEBUG             = os.environ.get("DEBUG", "1") not in ("0","false","False")
POLL_TIMEOUT_SEC  = float(os.environ.get("POLL_TIMEOUT_SEC", "1.0"))

# SMTP 재시도 파라미터
SMTP_MAX_TRY      = int(os.environ.get("SMTP_MAX_TRY", "5"))
SMTP_BASE_DELAY   = float(os.environ.get("SMTP_BASE_DELAY", "1.0"))
SMTP_MAX_DELAY    = float(os.environ.get("SMTP_MAX_DELAY", "30.0"))

# ================== Graceful Stop ==================
stop_flag = False
def handle_sigterm(signum, frame):
    global stop_flag
    stop_flag = True

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ================== Utils ==================
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def parse_json(b: Optional[bytes]) -> Dict[str, Any]:
    """bytes -> dict; 불량이면 {} 반환"""
    if b is None or len(b) == 0:
        return {}
    try:
        return json.loads(b.decode("utf-8", errors="replace"))
    except Exception:
        if DEBUG:
            # 200바이트까지만 로그
            snippet = b[:200] if isinstance(b, (bytes, bytearray)) else str(b)[:200]
            log(f"[warn] bad json: {snippet!r}")
        return {}

def safe_commit(consumer: Consumer, msg):
    """동기 커밋 + 예외 안전"""
    try:
        consumer.commit(msg, asynchronous=False)
    except Exception as e:
        # 커밋 실패는 재처리(중복 메일)로 이어질 수 있으므로 반드시 로깅
        log(f"[commit-fail] {type(e).__name__}: {e}")

# ================== Mail ==================
def html_mail_body(ticket_id: str, rec: Dict[str, Any]) -> str:
    name     = rec.get("name","")
    email    = rec.get("email","")
    company  = rec.get("company","")
    model    = rec.get("model","")
    sn       = rec.get("serial_number","")
    initial  = rec.get("initial_install_date","")
    failure  = rec.get("failure_date","") or "-"
    content  = rec.get("memo","") or "-"
    status   = rec.get("status") or rec.get("__op") or "c"

    return f"""\
<!DOCTYPE html>
<html lang="ko">
  <head>
    <meta charset="UTF-8" />
    <title>RMA 접수 안내</title>
  </head>
  <body style="margin:0;padding:0;background:#f7f8fb;color:#0b1020;
               font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
               'Noto Sans KR', 'Apple SD Gothic Neo', Arial, sans-serif;">
    <div style="max-width:680px;margin:24px auto;padding:24px;background:#ffffff;
                border:1px solid #e8ecf3;border-radius:16px;box-shadow:0 8px 30px rgba(0,0,0,.05)">
      <img src="cid:logo" alt="PIOLINK" width="140" height="auto"
           style="display:block;margin:0 0 12px 0" />
      <h2 style="margin:0 0 6px 0;">RMA 접수 안내</h2>
      <p style="margin:0 0 14px 0;">TICKET ID: <b>{ticket_id}</b></p>

      <table cellpadding="10" cellspacing="0" style="width:100%;border-collapse:collapse;background:#fbfcff;border:1px solid #e8ecf3;border-radius:10px;overflow:hidden">
        <tr style="background:#f2f6ff">
          <td style="width:160px;font-weight:700;border-bottom:1px solid #e8ecf3;">신청자</td>
          <td style="border-bottom:1px solid #e8ecf3;">{name} ({email})</td>
        </tr>
        <tr>
          <td style="font-weight:700;border-bottom:1px solid #e8ecf3;">회사</td>
          <td style="border-bottom:1px solid #e8ecf3;">{company}</td>
        </tr>
        <tr style="background:#f9fbff">
          <td style="font-weight:700;border-bottom:1px solid #e8ecf3;">모델</td>
          <td style="border-bottom:1px solid #e8ecf3;">{model}</td>
        </tr>
        <tr>
          <td style="font-weight:700;border-bottom:1px solid #e8ecf3;">시리얼</td>
          <td style="border-bottom:1px solid #e8ecf3;">{sn}</td>
        </tr>
        <tr style="background:#f9fbff">
          <td style="font-weight:700;border-bottom:1px solid #e8ecf3;">최초설치일</td>
          <td style="border-bottom:1px solid #e8ecf3;">{initial}</td>
        </tr>
        <tr>
          <td style="font-weight:700;border-bottom:1px solid #e8ecf3;">장애발생일</td>
          <td style="border-bottom:1px solid #e8ecf3;">{failure}</td>
        </tr>
        <tr style="background:#f9fbff">
          <td style="font-weight:700">상태</td>
          <td>{status}</td>
        </tr>
        <tr>
          <td style="font-weight:700">내용</td>
          <td>{content}</td>
        </tr>
      </table>

      <p style="margin:16px 0 0 0;">내용은 검토 후 담당자가 연락 드리겠습니다.</p>
      <p style="margin:6px 0 0 0;color:#6b7280;font-size:13px">본 메일은 자동 발송되었습니다.</p>
    </div>
  </body>
</html>
"""

def build_email(to_email: str, subject: str, html: str, logo_path: str = LOGO_PATH) -> EmailMessage:
    if not to_email:
        raise ValueError("empty recipient")

    msg = EmailMessage(policy=policy.default)
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = to_email

    msg.set_content("PIOLINK RMA 접수 안내드립니다.")
    msg.add_alternative(html, subtype="html")

    # 인라인 로고 (cid:logo)
    try:
        with open(logo_path, "rb") as f:
            data = f.read()
        html_part = msg.get_payload()[-1]  # 마지막이 text/html
        # cid는 angle bracket 없이 지정 (라이브러리가 내부적으로 감쌈)
        html_part.add_related(data, maintype="image", subtype="png", cid="logo")
    except FileNotFoundError:
        if DEBUG:
            log(f"[warn] logo not found at {logo_path}, skip attaching.")

    return msg

def send_email_ssl_message(msg: EmailMessage):
    """SMTP over SSL + 지수 백오프 재시도"""
    attempts, delay = 0, SMTP_BASE_DELAY
    last_exc = None

    while attempts < SMTP_MAX_TRY and not stop_flag:
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as s:
                # s.set_debuglevel(1)  # 필요 시 활성화
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                    log(f"[mail] logged in as {SMTP_USER}")
                resp = s.send_message(msg, from_addr=MAIL_FROM, to_addrs=[msg["To"]])
                log(f"[mail] sent to {msg['To']} subject={msg['Subject']} resp={resp}")
                return
        except Exception as e:
            attempts += 1
            last_exc = e
            log(f"[mail-retry] attempt={attempts} err={type(e).__name__}: {e}")
            time.sleep(min(delay, SMTP_MAX_DELAY))
            delay = min(delay * 2 + random.random(), SMTP_MAX_DELAY)

    raise last_exc if last_exc else RuntimeError("SMTP send failed")

# ================== Kafka Produce (DLQ, optional) ==================
_dlq_producer: Optional[Producer] = None

def get_dlq_producer() -> Optional[Producer]:
    global _dlq_producer
    if not DLQ_TOPIC:
        return None
    if _dlq_producer is None:
        try:
            _dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
        except Exception as e:
            log(f"[dlq-init-fail] {type(e).__name__}: {e}")
            _dlq_producer = None
    return _dlq_producer

def send_to_dlq(payload: Dict[str, Any], reason: str):
    """실패한 레코드를 DLQ 토픽으로 (가능할 때만)"""
    if not DLQ_TOPIC:
        return
    p = get_dlq_producer()
    if p is None:
        return
    try:
        data = json.dumps({"reason": reason, "payload": payload}, ensure_ascii=False).encode("utf-8")
        p.produce(DLQ_TOPIC, value=data)
        p.poll(0)
    except Exception as e:
        log(f"[dlq-fail] {type(e).__name__}: {e}")

# ================== Record Handling ==================
def handle_record(rec: Dict[str, Any]):
    """
    rec 예시:
      {
        "email": "...",
        "ticket_id": "...",        # 없으면 id 또는 record.id 사용
        "status": "...",
        "submitted_at_ms": 17605...,
        "record": {...원문...}
      }
    """
    to = rec.get("email") or rec.get("record", {}).get("email")
    ticket_id = rec.get("ticket_id") or rec.get("id") or rec.get("record", {}).get("id") or ""
    record = rec.get("record") or rec

    if not to:
        raise ValueError("no recipient email in payload")

    html = html_mail_body(ticket_id, record)
    subject = f"[RMA] 접수 안내 - {ticket_id}"
    msg = build_email(to, subject, html, LOGO_PATH)
    send_email_ssl_message(msg)  # 성공/실패는 상위에서 커밋 제어

# ================== Kafka Consume ==================
def on_assign(consumer, partitions: List[TopicPartition]):
    log(f"[assign] {[(p.topic, p.partition) for p in partitions]}")
    consumer.assign(partitions)

def on_revoke(consumer, partitions: List[TopicPartition]):
    log(f"[revoke] {[(p.topic, p.partition) for p in partitions]}")

def main():
    # Consumer 설정: 반드시 수동 커밋으로 (메일 성공 후 커밋)
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": NOTI_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
        "max.poll.interval.ms": 300000,
        # "debug": "cgrp,topic,broker",  # 파티션 할당받지 못하거나 잘안될 때 일단 디버깅모드 활성화해서 확인...

    }
    consumer = Consumer(conf)
    consumer.subscribe([NOTIFY_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log(f"[init] subscribe topic={NOTIFY_TOPIC} group={NOTI_GROUP_ID}")

    try:
        while not stop_flag:
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None:
                continue

            if msg.error():
                # 파티션 EOF는 노이즈, 이외는 경고
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log(f"[kafka-err] {msg.error()}")
                # 에러 레코드는 일반적으로 커밋하지 않음(재시도 유도)
                # 필요시 정책적으로 커밋 고려
                continue

            # 메시지 메타
            tp = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
            ts = msg.timestamp()
            if DEBUG:
                log(f"[recv] {tp} ts={ts}")

            # tombstone (압축토픽) 방어
            raw = msg.value()
            if raw is None:
                if DEBUG:
                    log(f"[skip] tombstone {tp}")
                safe_commit(consumer, msg)  # 정책상 tombstone은 커밋
                continue

            try:
                payload = parse_json(raw)
                if not payload:
                    raise ValueError("invalid/empty JSON payload")

                handle_record(payload)          # 메일 전송 (실패 시 예외)
                safe_commit(consumer, msg)      # 성공한 경우에만 커밋 (핵심)
            except Exception as e:
                # 실패 시 재처리될 수 있도록 커밋 금지
                reason = f"{type(e).__name__}: {e}"
                log(f"[mail-fail] {reason}")
                if DEBUG:
                    traceback.print_exc(file=sys.stderr)
                # DLQ 사용 시 기록
                try:
                    payload_for_dlq = payload if 'payload' in locals() and payload else {"raw": raw.decode("utf-8", errors="replace")}
                except Exception:
                    payload_for_dlq = {"raw": "<unprintable-bytes>"}
                send_to_dlq(payload_for_dlq, reason)

    except KeyboardInterrupt:
        pass
    except KafkaException as e:
        log(f"[kafka-exc] {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        # DLQ producer flush
        if _dlq_producer is not None:
            try:
                _dlq_producer.flush(5.0)
            except Exception:
                pass
        log("[exit] consumer closed")

if __name__ == "__main__":
    main()

