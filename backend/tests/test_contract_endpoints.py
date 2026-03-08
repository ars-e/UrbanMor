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


def test_transport_layer_endpoints_exist(client) -> None:
    cities = client.get("/cities")
    city_name = cities.json()["cities"][0]["city"]
    geojson = client.get(f"/cities/{city_name}/wards/geojson").json()
    coords: list[tuple[float, float]] = []

    def ingest(values):
        if isinstance(values, list) and values and isinstance(values[0], (int, float)):
            coords.append((float(values[0]), float(values[1])))
            return
        if isinstance(values, list):
            for item in values:
                ingest(item)

    ingest(geojson["features"][0]["geometry"]["coordinates"])
    west = min(x for x, _ in coords)
    east = max(x for x, _ in coords)
    south = min(y for _, y in coords)
    north = max(y for _, y in coords)
    bbox = f"{west},{south},{east},{north}"

    roads = client.get(f"/cities/{city_name}/roads/geojson", params={"bbox": bbox, "zoom": 11, "detail": "major"})
    assert roads.status_code == 200

    transit = client.get(f"/cities/{city_name}/transit/geojson", params={"bbox": bbox})
    assert transit.status_code == 200


def test_swagger_available(client) -> None:
    response = client.get("/docs")
    assert response.status_code == 200
