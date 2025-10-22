# runtime stage
FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

# 1) 의존성 레이어(캐시): package.json + lock만 먼저 복사
COPY package.json package-lock.json ./

# 2) lock이 있으니 ci 사용 (더 빠르고 재현성 보장)
RUN npm ci --omit=dev --no-audit --no-fund

# 3) 앱 파일 복사
COPY server.js index.html ./ 
COPY utils ./utils
COPY static ./static

# 이하 동일...
USER node
EXPOSE 8080
CMD ["node","server.js"]

