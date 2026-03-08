def test_required_endpoints_exist(client) -> None:
    cities = client.get("/cities")
    assert cities.status_code == 200
    city_name = cities.json()["cities"][0]["city"]
    geojson = client.get(f"/cities/{city_name}/wards/geojson")
    assert geojson.status_code == 200

    health = client.get("/health")
    assert health.status_code == 200

    meta = client.get("/meta/metrics")
    assert meta.status_code == 200

    missing_job = client.get("/analyse/jobs/does-not-exist")
    assert missing_job.status_code == 404


def test_swagger_available(client) -> None:
    response = client.get("/docs")
    assert response.status_code == 200
