from io import BytesIO
from unittest.mock import Mock
from zipfile import ZipFile

import pytest

from ingestion.cnig.ingest import CnigIngestion


def build_ngmep_zip(
    *,
    include_provinces=True,
    include_municipalities=True,
):
    buffer = BytesIO()

    with ZipFile(buffer, "w") as archive:
        if include_provinces:
            archive.writestr(
                "PROVINCIAS.csv",
                "COD_PROV;PROVINCIA\n01;Araba",
            )

        if include_municipalities:
            archive.writestr(
                "MUNICIPIOS.csv",
                "COD_GEO;MUNICIPIO\n01001;Alegria",
            )

    return buffer.getvalue()


def test_ingest_ngmep_persists_required_master_files():
    client = Mock()
    storage = Mock()

    client.download_ngmep_zip.return_value = (
        build_ngmep_zip()
    )

    provinces_path = (
        "bronze/cnig/provinces/"
        "provinces.csv"
    )

    municipalities_path = (
        "bronze/cnig/municipalities/"
        "municipalities.csv"
    )

    storage.save_bytes.side_effect = [
        provinces_path,
        municipalities_path,
    ]

    ingestion = CnigIngestion(
        client=client,
        storage=storage,
    )

    result = ingestion.ingest_ngmep()

    assert result == [
        provinces_path,
        municipalities_path,
    ]

    assert (
        storage.save_bytes.call_count
        == 2
    )

    first = (
        storage.save_bytes
        .call_args_list[0]
    )

    second = (
        storage.save_bytes
        .call_args_list[1]
    )

    assert first.kwargs[
        "source"
    ] == "cnig"

    assert first.kwargs[
        "dataset"
    ] == "provinces"

    assert first.kwargs[
        "object_name"
    ] == provinces_path

    assert first.kwargs[
        "content_type"
    ] == "text/csv"

    assert second.kwargs[
        "dataset"
    ] == "municipalities"

    assert second.kwargs[
        "object_name"
    ] == municipalities_path

    assert second.kwargs[
        "content_type"
    ] == "text/csv"


def test_ingest_ngmep_rejects_missing_required_file():
    client = Mock()
    storage = Mock()

    client.download_ngmep_zip.return_value = (
        build_ngmep_zip(
            include_municipalities=False,
        )
    )

    ingestion = CnigIngestion(
        client=client,
        storage=storage,
    )

    with pytest.raises(
        ValueError,
        match="MUNICIPIOS.csv",
    ):
        ingestion.ingest_ngmep()
