from os import getenv
from dotenv import load_dotenv
from src.infra.logger import Logger
from src.utils.aws_utils import AWSUtils
from src.utils.data_handler_utils import DataHandlerUtils
from src.service.scraper import B3Scraper

load_dotenv()

logger = Logger.get_logger(__name__)

def main():
    # AWS S3 Client setup
    aws = AWSUtils()
    aws_keys = aws.get_credentials() 
    s3_client = aws.create_s3_client()
    
    # Scraping process
    scraper = B3Scraper()
    data = scraper.extract_data()

    # Data handling and upload to S3
    df_handler = DataHandlerUtils(data, s3_client, getenv("BUCKET_B3_DATA_PATH"), aws_keys["BUCKET_NAME"])
    df_handler.to_parquet_and_upload()

if __name__ == "__main__":
    main()