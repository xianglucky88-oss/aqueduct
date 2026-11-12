# 多阶段构建。TARGET 传 mocktarget 或 collector。
ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-alpine AS build
ARG TARGET=collector
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/target ./cmd/${TARGET}

FROM alpine:3.19
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=build /out/target /app/target
COPY configs /app/configs
ENTRYPOINT ["/app/target"]
