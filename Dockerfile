FROM python:alpine
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app/
COPY pyproject.toml .
RUN uv sync --no-dev

COPY app.py .
ENTRYPOINT [ "uv", "run", "app.py" ]
