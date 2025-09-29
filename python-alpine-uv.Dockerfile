FROM python:alpine
RUN apk add uv
ENV UV_CACHE_DIR=/uv_cache_dir/
VOLUME [ "/uv_cache_dir" ]
WORKDIR /app/
# Note: This uv/docker approach is not suitable for production. The uv packages are downloaded on first run rather than the container actually being built with all the dependencies
# TODO:
#   A way to build prod containers with dependencies
#   A way to run tests? pytest?
