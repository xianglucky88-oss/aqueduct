# 多阶段构建。TARGET 传 mocktarget 或 collector。
ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-alpine AS build
ARG TARGET=collector
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO 关闭以获得静态二进制，容器基础镜像无需带 libc 依赖。
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/target ./cmd/${TARGET}

FROM alpine:3.19
RUN apk add --no-cache ca-certificates curl \
 && addgroup -g 10001 -S app \
 && adduser -u 10001 -S app -G app
WORKDIR /app
COPY --from=build /out/target /app/target
COPY configs /app/configs
# 以非 root 运行：Kubernetes 的 securityContext 会再次强制这一点。
USER 10001:10001
EXPOSE 8081 9100
ENTRYPOINT ["/app/target"]
