import json as json_lib
import os
import sys
from pathlib import Path
from types import ModuleType

# Define HTTPError and other exceptions
class HTTPError(Exception):
    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop("response", None)
        super().__init__(*args, **kwargs)

class RequestException(Exception):
    pass

# Dynamically set up requests.exceptions in sys.modules
exceptions_mod = ModuleType("requests.exceptions")
exceptions_mod.HTTPError = HTTPError
exceptions_mod.RequestException = RequestException
sys.modules["requests.exceptions"] = exceptions_mod

class Response:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def json(self):
        if isinstance(self.content, bytes):
            return json_lib.loads(self.content.decode("utf-8"))
        elif isinstance(self.content, str):
            return json_lib.loads(self.content)
        return self.content

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise HTTPError(f"HTTP Error {self.status_code}", response=self)

class MockSession:
    def __init__(self):
        self.auth = None
        self.headers = {}

    def request(self, method, url, params=None, json=None, timeout=None, **kwargs):
        # Support mock failure modes
        failure_mode = os.getenv("MOCK_AIRFLOW_FAILURE")
        if failure_mode == "exception":
            raise RequestException("Simulated connection error")
        elif failure_mode == "500":
            res = Response(500, b"Internal Server Error")
            self._log_request(method, url, params, json)
            return res
        elif failure_mode == "404":
            res = Response(404, b"Not Found")
            self._log_request(method, url, params, json)
            return res

        # Determine mock response based on URL
        status_code = 200
        content = b"{}"
        if "/dags/" in url and "/dagRuns" in url:
            # Triggering DAG
            content = json_lib.dumps({"dag_run_id": "mock_dag_run_123", "state": "queued"}).encode("utf-8")
        elif "/variables/" in url:
            # Getting or patching variable
            var_name = url.split("/variables/")[-1]
            content = json_lib.dumps({"key": var_name, "value": "{}"}).encode("utf-8")
        elif "/variables" in url:
            # POST variable
            content = json_lib.dumps({"key": "var", "value": "{}"}).encode("utf-8")
        elif "/dags" in url:
            content = json_lib.dumps({"dags": []}).encode("utf-8")

        res = Response(status_code, content)
        self._log_request(method, url, params, json)
        return res

    def _log_request(self, method, url, params, json_data):
        mock_dir_env = os.getenv("MOCK_DATA_DIR")
        if mock_dir_env:
            mock_dir = Path(mock_dir_env)
            mock_dir.mkdir(parents=True, exist_ok=True)
            req_file = mock_dir / "airflow_requests.json"
            
            existing_requests = []
            if req_file.exists():
                try:
                    with open(req_file, "r", encoding="utf-8") as f:
                        existing_requests = json_lib.load(f)
                        if not isinstance(existing_requests, list):
                            existing_requests = []
                except Exception:
                    pass
            
            existing_requests.append({
                "method": method,
                "url": url,
                "params": params,
                "json": json_data
            })
            
            with open(req_file, "w", encoding="utf-8") as f:
                json_lib.dump(existing_requests, f, indent=2, ensure_ascii=False)

def Session():
    return MockSession()
