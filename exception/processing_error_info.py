from dataclasses import dataclass
from http import HTTPStatus


@dataclass(frozen=True)
class ProcessingErrorInfo:
    http_status: HTTPStatus | None
    retryIndicator: bool | None
    
    error_type: str | None
    error_code: str | None
    description: str | None
    info_url: str | None

    raw_json: dict | None
