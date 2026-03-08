from __future__ import annotations


def _first_city(client) -> str:
    response = client.get('/cities')
    assert response.status_code == 200
    cities = response.json()['cities']
    assert cities
    return cities[0]['city']


def test_invalid_city_format_rejected(client) -> None:
    response = client.get('/cities/../../etc/wards')
    assert response.status_code in {400, 404}


def test_unknown_city_returns_not_found(client) -> None:
    response = client.get('/cities/nonexistent_city/wards')
    assert response.status_code == 404


def test_weak_data_metrics_do_not_break_payload(client) -> None:
    city = _first_city(client)

    wards = client.get(f'/cities/{city}/wards')
    assert wards.status_code == 200
    ward_id = wards.json()['wards'][0]['ward_id']

    ward = client.get(f'/cities/{city}/wards/{ward_id}')
    assert ward.status_code == 200
    payload = ward.json()

    assert 'quality_summary' in payload
    assert 'all_metrics' in payload['metrics_json']
    # weak-data metrics should remain representable (null/object values allowed)
    assert isinstance(payload['metrics_json']['all_metrics'], dict)


def test_large_polygon_async_submission_is_accepted(client) -> None:
    city = _first_city(client)

    payload = {
        'mode': 'custom_polygon',
        'city': city,
        'geometry': {
            'type': 'Polygon',
            'coordinates': [
                [
                    [70.0, 7.0],
                    [90.0, 7.0],
                    [90.0, 35.0],
                    [70.0, 35.0],
                    [70.0, 7.0],
                ]
            ],
        },
    }

    response = client.post('/analyse', json=payload)
    assert response.status_code == 202
    job = response.json()
    assert job['status'] in {'queued', 'running'}

    poll = client.get(f"/analyse/jobs/{job['job_id']}")
    assert poll.status_code == 200
