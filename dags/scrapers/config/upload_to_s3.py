from botocore.exceptions import NoCredentialsError
from scrapers.config.stores_info import FILE_NAME_DICT
from datetime import datetime
from scrapers.config.logger import LOGGER
from airflow.models import Variable
import os, boto3, shutil


def upload_file_to_s3(local_file, bucket, s3_file):
    """
    Uploads the data present in folder 'scrapers_data' to s3

    Args:
        local_file (str): The local file path
        bucket (str): The name of the bucket
        s3_file (str): The path of the file that will be saved in s3
    """

    # Loads the variables present in .env
    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
    SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )

    try:
        s3.upload_file(local_file, bucket, s3_file)
        LOGGER.info("upload_file_to_s3: Upload Successful")

        # clear_file_from_folder(local_file)

        return True

    except FileNotFoundError:
        LOGGER.error("upload_file_to_s3: The file was not found")

        return False

    except NoCredentialsError:
        LOGGER.error("upload_file_to_s3: Credentials not available")

        return False


def clear_file_from_folder(local_file):
    """
    Remove file from folder 'scrapers_data'

    Args:
        local_file (str): The local file path
    """
    folder = "scrapers_data/"

    for file_name in os.listdir(folder):
        file_path = os.path.join(folder, file_name)

        if file_path == local_file:
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)

                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

            except Exception as error:
                LOGGER.error(
                    f"clear_file_from_folder: Failed to delete {file_path}. Reason: {error}"
                )


def main():
    # Each CSV file present in the folder 'scrapers_data' is sent to s3 and removed from the folder afterwards
    for value in FILE_NAME_DICT.values():
        date = datetime.today().strftime("%Y-%m-%d")

        # Set the local file path
        file_name = date + value + ".csv"
        local_file_name = "scrapers_data/" + file_name

        # Set the path of the file that will be saved in s3
        s3_file_name = "scrapers_data/scania/" + date + "/" + file_name

        upload_file_to_s3(local_file_name, "la.aquare.data-tactics-pat", s3_file_name)
        
        clear_file_from_folder(local_file_name)


if __name__ == "__main__":
    main()
