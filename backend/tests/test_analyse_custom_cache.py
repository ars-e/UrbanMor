from __future__ import annotations

import time
from uuid import uuid4


def test_custom_polygon_cache_miss_then_hit(client) -> None:
    # jittered tiny polygon to avoid collisions with existing cache entries
    jitter = (uuid4().int % 1_000_000) / 1_000_000_000.0
    lon = 76.76 + jitter
    lat = 30.72 + jitter
    geom = {
        "type": "Polygon",
        "coordinates": [
            [
                [lon, lat],
                [lon + 0.0005, lat],
                [lon + 0.0005, lat + 0.0005],
                [lon, lat + 0.0005],
                [lon, lat],
            ]
        ],
    }

    payload = {
        "mode": "custom_polygon",
        "city": "chandigarh",
        "geometry": geom,
        "run_async": False,
    }

    first = client.post("/analyse", json=payload)
    assert first.status_code == 200
    first_json = first.json()
    assert first_json["mode"] == "custom_polygon"
    assert first_json["result"]["cache_hit"] is False

    second = client.post("/analyse", json=payload)
    assert second.status_code == 200
    second_json = second.json()
    assert second_json["result"]["cache_hit"] is True


def test_custom_polygon_async_polling(client) -> None:
    jitter = (uuid4().int % 1_000_000) / 1_000_000_000.0
    lon = 77.21 + jitter
    lat = 28.61 + jitter
    geom = {
        "type": "Polygon",
        "coordinates": [
            [
                [lon, lat],
                [lon + 0.0004, lat],
                [lon + 0.0004, lat + 0.0004],
                [lon, lat + 0.0004],
                [lon, lat],
            ]
        ],
    }

    enqueue = client.post(
        "/analyse",
        json={
            "mode": "custom_polygon",
            "city": "delhi",
            "geometry": geom,
            "run_async": True,
        },
    )
    assert enqueue.status_code == 202
    job = enqueue.json()
    assert "job_id" in job

    deadline = time.time() + 45
    terminal_payload = None
    while time.time() < deadline:
        poll = client.get(f"/analyse/jobs/{job['job_id']}")
        assert poll.status_code == 200
        payload = poll.json()
        if payload["status"] in {"succeeded", "failed"}:
            terminal_payload = payload
            break
        time.sleep(0.25)

    assert terminal_payload is not None
    assert terminal_payload["status"] == "succeeded"
    assert terminal_payload["progress_pct"] == 100
    assert terminal_payload["result"] is not None


def test_malformed_geometry_rejected(client) -> None:
    bad_payload = {
        "mode": "custom_polygon",
        "city": "chandigarh",
        "geometry": {"type": "Polygon", "coordinates": []},
        "run_async": False,
    }

    resp = client.post("/analyse", json=bad_payload)
    assert resp.status_code in {400, 422, 503}
