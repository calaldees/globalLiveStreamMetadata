FROM python:alpine
RUN apk add uv
ENV UV_CACHE_DIR=/uv_cache_dir/
VOLUME [ "/uv_cache_dir" ]
WORKDIR /app/