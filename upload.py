## Utility class for uploading data to BigQuery

from pydantic import BaseModel
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
from models import CFEntrant, CFScore
from parameters import GoogleCloudParameters
import time

gcp_params = GoogleCloudParameters()

class UploadManager:
    def __init__(
        self,
        client: bigquery.Client,
        model_type: BaseModel,
        full_table_id: str,
        key_fields: tuple[str, ...],
        columns: tuple[str, ...],
    ):
        self.client = client
        self.model_type = model_type
        self.full_table_id = full_table_id
        self.key_fields = key_fields
        self.columns = columns
        self.models = {}

    @staticmethod
    def _sql_table(table_id: str) -> str:
        """Backtick-quoted ``project.dataset.table`` (hyphenated project ids need quoting)."""
        return f"`{table_id}`"

    def add_models(self, models: list[BaseModel]):
        assert all(isinstance(model, self.model_type) for model in models)

        for model in models:
            key = tuple(getattr(model, field) for field in self.key_fields)
            self.models[key] = model

    def get_json_data(self) -> list[dict]:
        return [model.model_dump() for model in self.models.values()]

    @property
    def temp_table_id(self) -> str:
        return f"{self.full_table_id}_temp"

    def _create_temporary_table(self):
        target_table = self.client.get_table(self.full_table_id)
        schema = target_table.schema
        temp_table = bigquery.Table(self.temp_table_id, schema=schema)
        temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
        return self.client.create_table(temp_table, exists_ok=True)

    def _wait_for_table(self, table_id: str, retries: int = 5, delay_seconds: float = 0.2):
        for attempt in range(retries):
            try:
                return self.client.get_table(table_id)
            except Exception:
                if attempt == retries - 1:
                    raise
                time.sleep(delay_seconds * (2 ** attempt))

    def _upload_to_temp_table(self):
        rows = self.get_json_data()
        if not rows:
            return []
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = self.client.load_table_from_json(
            rows,
            self.temp_table_id,
            job_config=job_config,
        )
        load_job.result()
        if load_job.errors:
            raise RuntimeError(f"BigQuery load_table_from_json errors: {load_job.errors}")
        return load_job

    def _merge(self, overwrite: bool = False):
        t = self.full_table_id
        s = self.temp_table_id
        # ON must be one boolean expression: use AND between key predicates (not commas).
        key_fields_str = " AND ".join([f"T.{field} = S.{field}" for field in self.key_fields])
        insert_cols = ", ".join(self.columns)
        insert_vals = ", ".join([f"S.{c}" for c in self.columns])
        update_fields = tuple(c for c in self.columns if c not in self.key_fields)
        update_fields_str = ", ".join([f"T.{field} = S.{field}" for field in update_fields])
        if overwrite and update_fields_str:
            overwrite_sql = f"WHEN MATCHED THEN\n  UPDATE SET {update_fields_str}\n"
        else:
            overwrite_sql = ""
        merge_query = f"""
MERGE `{t}` T
USING `{s}` S
ON {key_fields_str}
{overwrite_sql}WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_vals})
"""
        return self.client.query(merge_query).result()

    def _delete_temp_table(self):
        return self.client.delete_table(self.temp_table_id, not_found_ok=True)

    def upload_and_merge(self, overwrite: bool = False):
        self._delete_temp_table()
        self._create_temporary_table()
        self._wait_for_table(self.temp_table_id)
        self._upload_to_temp_table()
        self._merge(overwrite=overwrite)
        self._delete_temp_table()

class CFEntrantUploadManager(UploadManager):
    def __init__(self):
        project_id = gcp_params.project_id
        staging_dataset_id = gcp_params.staging_dataset_id
        super().__init__(
            client=bigquery.Client(project=project_id),
            model_type=CFEntrant,
            full_table_id=f"{project_id}.{staging_dataset_id}.cf_entrants",
            key_fields=("comp_id", "division_id", "cf_id"),
            columns=tuple(CFEntrant.model_fields.keys()),
        )

class CFScoreUploadManager(UploadManager):
    def __init__(self):
        project_id = gcp_params.project_id
        staging_dataset_id = gcp_params.staging_dataset_id
        super().__init__(
            client=bigquery.Client(project=project_id),
            model_type=CFScore,
            full_table_id=f"{project_id}.{staging_dataset_id}.cf_scores",
            key_fields=("comp_id", "division_id", "cf_id", "ordinal"),
            columns=tuple(CFScore.model_fields.keys()),
        )