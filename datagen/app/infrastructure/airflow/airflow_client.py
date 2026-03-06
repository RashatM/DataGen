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

    def post_with_retry(
            self,
            url: str,
            payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    auth=self.auth,
                    verify=False,
                )
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as error:
                last_error = error
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_base ** attempt)

        raise RuntimeError(
            f"Airflow POST failed after {self.config.max_retries} attempts"
        ) from last_error

    def get_with_retry(self, url: str) -> Dict[str, Any]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = requests.get(url, auth=self.auth, verify=False)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as error:
                last_error = error
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_base ** attempt)

        raise RuntimeError(
            f"Airflow GET failed after {self.config.max_retries} attempts"
        ) from last_error

    def trigger_dag(
            self,
            dag_run_id: str,
            payload: Dict[str, Any],
    ) -> None:
        url = f"{self.config.base_url}/api/v1/dags/{self.config.dag_id}/dagRuns"
        body = {"dag_run_id": dag_run_id, "conf": payload}
        self.post_with_retry(url=url, payload=body)

    def get_dag_run_state(self, dag_run_id: str) -> DagRunState:
        url = (
            f"{self.config.base_url}/api/v1/dags/"
            f"{self.config.dag_id}/dagRuns/{dag_run_id}"
        )
        raw = self.get_with_retry(url=url)
        return DagRunState(
            dag_run_id=raw.get("dag_run_id", dag_run_id),
            state=raw.get("state", ""),
            raw=raw,
        )
