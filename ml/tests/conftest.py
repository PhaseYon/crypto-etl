import pytest


@pytest.fixture(autouse=True)
def isolated_mlflow(tmp_path, monkeypatch):
    """Every test gets its own MLflow store, so nothing is written into the repo."""
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path}/mlflow/mlflow.db")
    monkeypatch.setenv("MLFLOW_ARTIFACT_ROOT", str(tmp_path / "mlflow" / "artifacts"))
    monkeypatch.setenv("MLFLOW_DISABLE_AGENT_HINT", "1")
    return tmp_path / "mlflow"
