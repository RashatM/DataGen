import time
from typing import Any, Dict, Optional
import urllib3
import requests
from requests.auth import HTTPBasicAuth

from app.infrastructure.dto import DagRunState
from app.shared.config import AirflowConfig

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AirflowClient:

    def __init__(self, config: AirflowConfig) -> None:
        self.config = config
        self.auth = HTTPBasicAuth(config.username, config.password)

    def dag_id(self) -> str:
        return self.config.dag_id

    def poll_interval(self) -> int:
        return self.config.poll_interval_seconds

    def build_dag_run_id(self, run_id: str) -> str:
        return f"{self.config.dag_run_id_prefix}_{run_id}"

    def request_with_retry(
            self,
            method: str,
            url: str,
            **kwargs,
    ) -> Dict[str, Any]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = requests.request(
                    method, url, auth=self.auth, verify=False, **kwargs,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as error:
                last_error = error
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_base ** attempt)

        raise RuntimeError(
            f"Airflow {method.upper()} failed after {self.config.max_retries} attempts"
        ) from last_error

    def trigger_dag(
            self,
            dag_run_id: str,
            payload: Dict[str, Any],
    ) -> None:
        url = f"{self.config.url}/api/v1/dags/{self.config.dag_id}/dagRuns"
        body = {"dag_run_id": dag_run_id, "conf": payload}
        self.request_with_retry("POST", url=url, json=body)

    def get_dag_run_state(self, dag_run_id: str) -> DagRunState:
        url = (
            f"{self.config.url}/api/v1/dags/"
            f"{self.config.dag_id}/dagRuns/{dag_run_id}"
        )
        raw = self.request_with_retry("GET", url=url)
        return DagRunState(
            dag_run_id=raw.get("dag_run_id", dag_run_id),
            state=raw.get("state", ""),
            raw=raw,
        )
