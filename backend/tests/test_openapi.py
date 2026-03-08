def test_openapi_available(client) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    payload = response.json()
    assert payload["info"]["title"] == "UrbanMor Backend"
