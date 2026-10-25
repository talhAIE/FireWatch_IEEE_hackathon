# Shared image; include only reviewed public deployment artifacts, never training archives/secrets.
FROM python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70 AS build
WORKDIR /build
COPY requirements-dev.lock ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-dev.lock
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m build --wheel --no-isolation

FROM python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70 AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV RAW_DATA_DIR=/var/lib/firewatch/raw MODEL_DIR=/var/lib/firewatch/models \
    REPLAY_BUNDLE_DIR=/var/lib/firewatch/replays MPLCONFIGDIR=/tmp/matplotlib \
    SOURCE_ARTIFACT_DIR=/app/deploy/render/assets
WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock \
    && groupadd --gid 10001 firewatch \
    && useradd --uid 10001 --gid firewatch --no-create-home firewatch \
    && mkdir -p /var/lib/firewatch/raw /var/lib/firewatch/models /var/lib/firewatch/replays \
    && chown -R firewatch:firewatch /var/lib/firewatch
COPY --from=build /build/dist/*.whl /tmp/wheels/
RUN python -m pip install --no-cache-dir --no-deps /tmp/wheels/*.whl \
    && python -m pip check && rm -rf /tmp/wheels
COPY alembic.ini ./
COPY migrations ./migrations
COPY configs ./configs
COPY deploy/render ./deploy/render
# Support both the container default and explicit MODEL_DIR=deploy/render/model.
COPY deploy/render/model /var/lib/firewatch/models
COPY scripts/render_assets.py ./scripts/render_assets.py
# Fail the build on missing bytes or an incompatible inference runtime.
RUN python scripts/render_assets.py verify --runtime
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["python", "-m", "firewatch.runtime"]
CMD ["uvicorn", "firewatch.main:app", "--host", "0.0.0.0", "--port", "8000"]
