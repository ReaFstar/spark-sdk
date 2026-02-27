# 构建阶段：使用官方 Rust 瘦身镜像，保证编译环境一致性
FROM rust:slim-bookworm AS builder

# 设置构建参数（Jenkins 传递镜像标签）
ARG IMAGE_TAG=latest

# 安装编译依赖（与文档一致）
RUN apt-get update -qq && \
    apt-get install -qq -y --no-install-recommends \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# 创建工作目录（严格匹配源码结构）
RUN mkdir -p /app/crates/breez-sdk/lnurl/

# 复制源码（Jenkins 会将仓库根目录挂载到 /app）
COPY . /app

# 进入工作目录，编译发布版
WORKDIR /app/crates/breez-sdk/lnurl
RUN cargo build --release --bin lnurl

# 运行阶段：使用 Debian 瘦身镜像，减小最终镜像体积
FROM debian:bookworm-slim AS final

# 安装运行依赖（证书、OpenSSL，保证网络和 HTTPS 正常）
RUN apt-get update -qq && \
    apt-get install -qq -y --no-install-recommends \
    ca-certificates \
    openssl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 创建非 root 用户，提升安全性（生产环境必备）
RUN useradd -m -s /bin/bash lnurl && \
    mkdir -p /data && \
    chown lnurl:lnurl /data

# 切换非 root 用户
USER lnurl

# 从构建阶段复制编译好的二进制文件
COPY --from=builder /app/crates/breez-sdk/lnurl/target/release/lnurl /usr/local/bin/

# 暴露服务端口（与文档一致）
EXPOSE 3001

# 数据卷（持久化 SQLite 或日志，生产环境建议 PostgreSQL）
VOLUME ["/data"]

# 入口点（保持与源码一致，通过环境变量配置）
ENTRYPOINT ["/usr/local/bin/lnurl"]