import importlib.util
import socket
from pathlib import Path

import pandas as pd
import pytest


def load_compute_module():
    proj_root = Path(__file__).resolve().parents[1]
    script_path = proj_root / "scripts" / "compute_player_stock.py"
    spec = importlib.util.spec_from_file_location(
        "compute_player_stock", str(script_path)
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec for {script_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def test_api_integration_and_espnid(tmp_path):
    """Integration-ish test:
    - write a tiny CSV with espn_id,name,epa,cpoe
    - run compute_player_stock_summary to generate summary CSV
    - assert espnId column exists in produced CSV
    - optionally query localhost:3000/api/nfl/stocks and assert espnId appears if server is running
    """
    mod = load_compute_module()
    compute = mod.compute_player_stock_summary

    # prepare sample CSV
    input_csv = tmp_path / "sample.csv"
    out_csv = tmp_path / "player_stock_summary.csv"
    df = pd.DataFrame(
        {
            "espn_id": ["9000", "9001"],
            "name": ["Test Player A", "Test Player B"],
            "epa": [0.5, -0.2],
            "cpoe": [0.1, -0.1],
        }
    )
    df.to_csv(input_csv, index=False)
    # run computation
    compute(str(input_csv), str(out_csv))

    # read back written file and assert espnId exists
    written = pd.read_csv(out_csv)
    assert "espnId" in written.columns
    # compare as strings (pandas may infer ints)
    assert list(written["espnId"].astype(str)) == ["9000", "9001"]

    # optional: hit running dev server and check API JSON
    host = "127.0.0.1"
    port = 3000
    if not is_port_open(host, port):
        pytest.skip("Dev server not running on localhost:3000; skipping API assertion")

    # if server is running, perform a lightweight HTTP GET
    try:
        import requests
    except Exception:
        pytest.skip("requests not installed; skipping API request")

    try:
        resp = requests.get(f"http://{host}:{port}/api/nfl/stocks", timeout=2.0)
        assert resp.status_code == 200
        payload = resp.json()
        # payload rows should include espnId on at least one row
        rows = payload.get("rows") or []
        assert any(
            (("espnId" in r and r["espnId"]) for r in rows)
        ), "No espnId found in API rows"
    except requests.RequestException:
        pytest.skip("Failed to reach /api/nfl/stocks; skipping API assertion")
