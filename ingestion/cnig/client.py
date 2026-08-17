"""
Client for the CNIG / IGN download service.
"""

from ingestion.common.config import (
    CNIG_BASE_URL,
    CNIG_NGMEP_LICENSE,
    CNIG_NGMEP_SEQUENTIAL,
    CNIG_NGMEP_SERIES,
)
from ingestion.common.exceptions import APIResponseError
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class CnigClient:
    """
    Client used to retrieve the CNIG / IGN NGMEP dataset.
    """

    def __init__(
        self,
        http_client: HTTPClient | None = None,
    ) -> None:
        self.http_client = http_client or HTTPClient()

    @property
    def headers(self) -> dict[str, str]:
        """
        Return the headers used by the CNIG download service.
        """

        return {
            "User-Agent": "Mozilla/5.0",
            "Referer": (
                f"{CNIG_BASE_URL}/detalleArchivo?"
                f"sec={CNIG_NGMEP_SEQUENTIAL}"
            ),
        }

    def _initialize_ngmep_download(self) -> str:
        """
        Initialize the NGMEP download and return its sequence.
        """

        endpoint = f"{CNIG_BASE_URL}/initDescargaDir"

        logger.info(
            "Initializing CNIG NGMEP download."
        )

        response = self.http_client.post(
            endpoint,
            data={
                "secuencial": CNIG_NGMEP_SEQUENTIAL,
            },
            headers=self.headers,
        )

        try:
            data = response.json()
        except ValueError as exc:
            raise APIResponseError(
                "Invalid JSON response received while "
                "initializing CNIG NGMEP download."
            ) from exc

        if not isinstance(data, dict):
            raise APIResponseError(
                "Unexpected CNIG initialization response format."
            )

        sequence = data.get("secuencialDescDir")

        if not sequence:
            raise APIResponseError(
                "CNIG initialization response does not contain "
                "'secuencialDescDir'."
            )

        return str(sequence)

    def download_ngmep_zip(self) -> bytes:
        """
        Download the raw NGMEP ZIP file from CNIG / IGN.
        """

        sequence = self._initialize_ngmep_download()

        endpoint = f"{CNIG_BASE_URL}/descargaDir"

        logger.info(
            "Downloading CNIG NGMEP ZIP."
        )

        response = self.http_client.post(
            endpoint,
            data={
                "secencial": sequence,
                "urlCart": "",
                "secDescDirLA": sequence,
                "codSerie": CNIG_NGMEP_SERIES,
                "id_productor": "",
                "codNumMD": "",
                "avisoLimiteFiles": "",
                "licenciaSeleccionada": CNIG_NGMEP_LICENSE,
            },
            headers=self.headers,
        )

        content = response.content

        if not content:
            raise APIResponseError(
                "Empty CNIG NGMEP ZIP response."
            )

        if not content.startswith(b"PK\x03\x04"):
            raise APIResponseError(
                "CNIG NGMEP response is not a valid ZIP payload."
            )

        logger.info(
            "CNIG NGMEP ZIP downloaded successfully. Bytes=%s",
            len(content),
        )

        return content