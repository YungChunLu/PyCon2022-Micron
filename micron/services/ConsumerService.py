import google.auth
import uuid
import json
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage_v1.types import ReadSession
from google.cloud.bigquery_storage import types
from google.cloud.bigquery_storage_v1.reader import ReadRowsStream


class ConsumerService:
    def __init__(self) -> None:
        self.client = BigQueryReadClient()
        _, self.project_id = google.auth.default()

    def get_session(
        self,
        dataset: str,
        table: str,
        project: str = "bigquery-public-data",
        max_stream_count: int = 1,
        selected_fields: List[str] = [],
        row_restriction: str = "",
    ) -> ReadSession:
        requested_session = types.ReadSession(  # type: ignore
            table=f"projects/{project}/datasets/{dataset}/tables/{table}",
            data_format=types.DataFormat.AVRO,  # type: ignore
            read_options={
                "selected_fields": selected_fields,
                "row_restriction": row_restriction,
            },
        )
        return self.client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=requested_session,
            max_stream_count=max_stream_count,
        )

    def download(
        self, dir_path: str, stream_name: str
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        reader: ReadRowsStream = self.client.read_rows(stream_name)
        _dir = Path(dir_path)
        _dir.mkdir(exist_ok=True, parents=True)
        fname = f"{uuid.uuid4()}.avro"
        fpath = _dir / fname
        schema = None
        with fpath.open("wb") as f:
            for record in reader:
                if record._pb.WhichOneof("schema"):
                    schema = json.loads(record.avro_schema.schema)
                f.write(record.avro_rows.serialized_binary_rows)
        return (fname, schema)
