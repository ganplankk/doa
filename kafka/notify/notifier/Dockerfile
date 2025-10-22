FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY noti.py .

# 이미지/첨부 파일을 담는 폴더 복사 (주석은 별도 줄!)
COPY assets/ ./assets/

ENV PYTHONUNBUFFERED=1
CMD ["python","-u","noti.py"]

