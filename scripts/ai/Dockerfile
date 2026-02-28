FROM oven/bun:alpine
RUN apk add --no-cache openssl
WORKDIR /app
COPY package.json *.ts ./
RUN bun install --production
CMD ["sh", "-c", "openssl req -x509 -nodes -newkey rsa:2048 -keyout /tmp/k.pem -out /tmp/c.pem -days 1 -subj '/CN=api-proxy' -addext 'subjectAltName=DNS:api-proxy' 2>/dev/null && exec bun proxy.ts"]
