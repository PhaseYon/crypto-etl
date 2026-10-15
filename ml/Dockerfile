FROM python:3.14.4-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MLFLOW_DISABLE_AGENT_HINT=1 \
    # No git in the image; stop MLflow warning about it
    GIT_PYTHON_REFRESH=quiet \
    # Tracking DB and artifacts live on the /data volume so train and predict containers share them
    MLFLOW_TRACKING_URI=sqlite:////data/mlruns/mlflow.db \
    MLFLOW_ARTIFACT_ROOT=/data/mlruns/artifacts

RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data/mlruns \
    && chown -R app:app /data

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY crypto_ml ./crypto_ml

USER app
VOLUME ["/data"]
ENTRYPOINT ["python", "-m"]
CMD ["crypto_ml.train", "--help"]
