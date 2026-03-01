FROM oven/bun:alpine
RUN apk add --no-cache openssl
WORKDIR /app
COPY package.json *.ts ./
RUN bun install --production
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
EXPOSE 8080 8083
ENTRYPOINT ["./entrypoint.sh"]
