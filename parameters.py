import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise OSError(
            f"Environment variable {name} is not set or empty. "
            "Set it in the environment or in a .env file loaded by dotenv."
        )
    return value


@dataclass
class GoogleCloudParameters:
    project_id: str = field(default_factory=lambda: _env_required("GCP_PROJECT_ID"))
    bucket_name: str = field(default_factory=lambda: _env_required("GCS_BUCKET_NAME"))