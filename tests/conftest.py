import shutil
import socket
from pathlib import Path

import pytest

from nlpipe.catalog import load_catalog
from nlpipe.ir import load

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Tests must use controlled transports; external network disabled")

    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket.socket, "connect", denied)


@pytest.fixture
def catalog():
    return load_catalog(ROOT / "examples/catalog.yaml")


@pytest.fixture
def spec():
    return load(ROOT / "examples/orders.yaml")


@pytest.fixture
def data_root(tmp_path):
    root = tmp_path / "data"
    shutil.copytree(ROOT / "examples/fixtures", root)
    return root


def pytest_collection_modifyitems(items):
    import importlib.util

    if importlib.util.find_spec("airflow") is None:
        skip = pytest.mark.skip(
            reason="Airflow extra absent; mandatory Airflow CI job runs this gate"
        )
        for item in items:
            if "airflow" in item.keywords:
                item.add_marker(skip)
