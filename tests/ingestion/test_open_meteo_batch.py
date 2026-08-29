from ingestion.open_meteo.batch import (
    OpenMeteoBatchIngestion,
)


def test_batch_size():
    ingestion = OpenMeteoBatchIngestion(
        http_client=object(),
        storage=object(),
        batch_size=100,
    )

    locations = [
        {"station_id": str(index)}
        for index in range(921)
    ]

    batches = list(
        ingestion._batches(locations)
    )

    assert len(batches) == 10
    assert len(batches[0]) == 100
    assert len(batches[-1]) == 21


def test_multi_response_count_validation():
    result = (
        OpenMeteoBatchIngestion
        ._normalize_response(
            [{}, {}],
            2,
        )
    )

    assert len(result) == 2
