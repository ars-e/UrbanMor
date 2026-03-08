def test_health_shape_and_observability(client) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] in {"ok", "degraded"}
    assert "database" in payload["checks"]
    assert "observability" in payload
    assert "query_timing" in payload["observability"]


def test_request_timing_header_present(client) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert "X-Request-Duration-Ms" in response.headers
