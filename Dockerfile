FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY src/ src/
COPY configs/ configs/

RUN uv sync --no-dev --frozen
ENV PATH="/app/.venv/bin:$PATH"
