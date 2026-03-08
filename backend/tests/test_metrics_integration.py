from __future__ import annotations


def _get_first_city_and_ward(client) -> tuple[str, str]:
    cities_resp = client.get("/cities")
    assert cities_resp.status_code == 200
    cities = cities_resp.json()["cities"]
    assert cities, "No cities returned"

    city = next((c["city"] for c in cities if c["cached_wards"] > 0), cities[0]["city"])
    wards_resp = client.get(f"/cities/{city}/wards")
    assert wards_resp.status_code == 200
    wards = wards_resp.json()["wards"]
    assert wards, f"No wards returned for {city}"

    return city, wards[0]["ward_id"]


def test_metric_endpoints_integration(client) -> None:
    city, ward_id = _get_first_city_and_ward(client)

    ward_resp = client.get(f"/cities/{city}/wards/{ward_id}")
    assert ward_resp.status_code == 200
    ward_payload = ward_resp.json()
    assert ward_payload["city"] == city
    assert ward_payload["ward_id"] == ward_id
    assert "all_metrics" in ward_payload["metrics_json"]

    city_metrics_resp = client.get(f"/cities/{city}/metrics")
    assert city_metrics_resp.status_code == 200
    city_metrics = city_metrics_resp.json()
    assert city_metrics["city"] == city
    assert city_metrics["ward_count"] > 0


def test_analyse_city_and_ward_paths(client) -> None:
    city, ward_id = _get_first_city_and_ward(client)

    ward_analyse = client.post(
        "/analyse",
        json={"mode": "ward", "city": city, "ward_id": ward_id},
    )
    assert ward_analyse.status_code == 200
    assert ward_analyse.json()["mode"] == "ward"

    city_analyse = client.post(
        "/analyse",
        json={"mode": "city", "city": city},
    )
    assert city_analyse.status_code == 200
    assert city_analyse.json()["mode"] == "city"
