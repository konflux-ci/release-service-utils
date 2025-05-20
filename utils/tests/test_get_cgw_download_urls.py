import pytest
import requests
from unittest.mock import Mock
from get_cgw_download_urls import list_download_urls, call_cgw_api, get_version_id


@pytest.fixture
def mock_file_data():
    return [
        {
            "id": 3886902,
            "description": "Helm",
            "label": "Checksum - Signature",
            "shortURL": "/pub/openshift-v4/clients/helm/3.15.4/sha256sum.txt.sig",
            "downloadURL": "/content/origin/files/sha256/4f/\4f6b0af28e8193bfa8b48f93096\
                abe6a11cbc97589d81b339ca7cc37b7f92d3c/sha256sum.txt.sig",
            "sha256": "4f6b0af28e8193bfa8b48f93096abe6a11cbc97589d81b339ca7cc37b7f92d3c",
            "size": 2095,
        },
        {
            "id": 3886930,
            "description": "Helm",
            "label": "Linux 64-bit (amd64)",
            "shortURL": "/pub/openshift-v4/clients/helm/3.15.4/helm-linux-amd64",
            "downloadURL": "/content/origin/files/sha256/c6/c6ff9aa942d710e73c877d765b76\
                82bc22fcdbc59e43d708511ba21d249696c7/helm-linux-amd64",
            "sha256": "c6ff9aa942d710e73c877d765b7682bc22fcdbc59e43d708511ba21d249696c7",
            "size": 52549927,
        },
        {
            "id": 3886844,
            "description": "Helm",
            "label": "Linux 64-bit Archive (amd64)",
            "shortURL": "/pub/openshift-v4/clients/helm/3.15.4/helm-linux-amd64.tar.gz",
            "downloadURL": "/content/origin/files/sha256/d3/d305ee5018571f2aca631da5faf4\
                c87eb5ceced40ec59d134b7d2dd166b82bc6/helm-linux-amd64.tar.gz",
            "sha256": "d305ee5018571f2aca631da5faf4c87eb5ceced40ec59d134b7d2dd166b82bc6",
            "size": 16453362,
        },
    ]


def test_list_download_urls(monkeypatch, capsys, mock_file_data):
    def mock_api(*args, **kwargs):
        return mock_file_data

    monkeypatch.setattr("get_cgw_download_urls.call_cgw_api", mock_api)
    list_download_urls("http://mock", None, 1, 1)
    output = capsys.readouterr().out.strip().split("\n")
    assert len(output) == 3
    for url in output:
        assert url.startswith("/content/origin/files/sha256/")


def test_retry_logic():
    class FailingSession:
        def __init__(self):
            self.attempts = 0

        def get(self, url):
            self.attempts += 1
            if self.attempts < 3:
                raise requests.RequestException("Temporary error")
            mock_response = Mock()
            mock_response.raise_for_status = lambda: None
            mock_response.json.return_value = []
            return mock_response

    session = FailingSession()
    result = call_cgw_api("http://mock", "/products", session)
    assert result == []
    assert session.attempts == 3


def test_get_version_id_success(monkeypatch):
    mock_versions = [
        {"versionName": "3.15.3", "id": 100},
        {"versionName": "3.15.4", "id": 101},
    ]

    def mock_call_cgw_api(host, endpoint, session, retries=5, delay=1):
        assert endpoint == "/products/123/versions"
        return mock_versions

    monkeypatch.setattr("get_cgw_download_urls.call_cgw_api", mock_call_cgw_api)
    version_id = get_version_id("http://mock", None, 123, "3.15.4")
    assert version_id == 101


def test_get_version_id_not_found(monkeypatch):
    monkeypatch.setattr("get_cgw_download_urls.call_cgw_api", lambda *a, **kw: [])
    with pytest.raises(ValueError, match="Version '3.15.9' not found"):
        get_version_id("http://mock", None, 123, "3.15.9")
