# 阶段1：构建 Rust 二进制文件（多阶段构建，减小最终镜像体积）
FROM rust:1.81-slim AS builder

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制 Cargo 配置（利用缓存加速构建）
COPY Cargo.toml Cargo.lock ./
COPY crates/breez-sdk/lnurl/Cargo.toml ./crates/breez-sdk/lnurl/

# 预编译依赖（空文件占位）
RUN mkdir -p ./crates/breez-sdk/lnurl/src && echo "fn main() {}" > ./crates/breez-sdk/lnurl/src/main.rs
RUN cargo build --release --manifest-path crates/breez-sdk/lnurl/Cargo.toml
RUN rm -rf ./crates/breez-sdk/lnurl/src

# 复制源码
COPY . .

# 正式构建
RUN cargo build --release --manifest-path crates/breez-sdk/lnurl/Cargo.toml

# 阶段2：生成最终镜像（精简版）
FROM debian:bookworm-slim

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libprotobuf32 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 创建非 root 用户（K8s 最佳实践）
RUN useradd -m -u 1000 lnurl
USER lnurl

# 设置工作目录
WORKDIR /home/lnurl

# 从构建阶段复制二进制文件
COPY --from=builder /app/target/release/lnurl ./

# 暴露端口
EXPOSE 3001

# 健康检查（K8s 探针用）
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://127.0.0.1:8080/lnurlp/health || exit 1

# 启动命令
ENTRYPOINT ["./lnurl"]