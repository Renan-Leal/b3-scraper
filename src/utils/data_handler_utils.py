import pandas as pd
from datetime import datetime
from io import BytesIO
from ..infra.logger import Logger

logger = Logger.get_logger(__name__)

class DataHandlerUtils:
    def __init__(self, data, s3_client, s3_data_path, s3_bucket_name):
        self.s3_client = s3_client
        self.s3_data_path = s3_data_path
        self.s3_bucket_name = s3_bucket_name
        self.table_columns = ["Code", "Stock", "Type", "Theoretical Qty", "Participation (%)"]
        self.df = pd.DataFrame(data, columns=self.table_columns)

    def to_parquet_and_upload(self):
        today = datetime.now()
        timestamp = datetime.now().timestamp()
        file_name = f"{today.year}{today.month:02d}{today.day:02d}/{timestamp}.parquet"
        s3_key = f"{self.s3_data_path}/{file_name}"

        buffer = BytesIO()
        self.df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.s3_client.upload_fileobj(buffer, self.s3_bucket_name, s3_key)
        logger.info(f"📂 File uploaded to {self.s3_data_path} dir on S3: {file_name}")