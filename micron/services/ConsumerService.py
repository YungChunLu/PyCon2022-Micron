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
        """Get storage read session from BQ

        Args:
            dataset (str): the source dataset name
            table (str): the source table name
            project (str, optional): the source GCP project name. Defaults to "bigquery-public-data".
            max_stream_count (int, optional): the number of reading streams. Defaults to 1.
            selected_fields (List[str], optional): the list of column names. Defaults to [].
            row_restriction (str, optional): the WHERE condition when filtering rows. Defaults to "".

        Returns:
            ReadSession: a storage read session
        """
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
        """Iter through the given read stream

        Args:
            dir_path (str): the data destination directory
            stream_name (str): the name of a read stream

        Returns:
            Tuple[str, Optional[Dict[str, Any]]]: (file name, json schema)
        """
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
