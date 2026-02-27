# 构建阶段：使用官方 Rust 瘦身镜像
FROM rust:slim-bookworm AS builder

# 安装编译依赖
RUN apt-get update -qq && \
    apt-get install -qq -y --no-install-recommends \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# 创建工作目录
WORKDIR /app

# 关键优化：先复制 Cargo.toml 和 Cargo.lock，下载依赖
COPY ./Cargo.toml ./Cargo.lock ./
# 为了让 cargo 能找到依赖，创建一个空的 src 目录
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
# 这一步会下载所有依赖，并缓存到 Docker 层
RUN cargo fetch --locked

# 现在再复制真正的源码
COPY . .

# 进入工作目录，编译发布版
WORKDIR /app/crates/breez-sdk/lnurl
RUN cargo build --release --bin lnurl

# 运行阶段：使用 Debian 瘦身镜像
FROM debian:bookworm-slim AS final

# 安装运行依赖
RUN apt-get update -qq && \
    apt-get install -qq -y --no-install-recommends \
    ca-certificates \
    openssl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 创建非 root 用户
RUN useradd -m -s /bin/bash lnurl && \
    mkdir -p /data && \
    chown lnurl:lnurl /data

# 切换非 root 用户
USER lnurl

# 从构建阶段复制编译好的二进制文件
COPY --from=builder /app/crates/breez-sdk/lnurl/target/release/lnurl /usr/local/bin/

# 暴露服务端口
EXPOSE 3001

# 数据卷
VOLUME ["/data"]

# 入口点
ENTRYPOINT ["/usr/local/bin/lnurl"]