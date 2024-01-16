from prefect import flow
from stream_file_validator import main
from bento.common.secret_manager import get_secret


SUBMISSION_BUCKET = "submission_bucket"


@flow(name="CRDC Data Hub File Validator", log_prints=True)
def data_hub_file_validator(
        organization_id,
        submission_id,
        manifest_name,
        secret_name,
        file_name_column="file_name",
        file_size_column="file_size",
        file_md5_column="md5sum"
):
    secret = get_secret(secret_name)
    bucket_name = secret[SUBMISSION_BUCKET]
    file_prefix = f"{organization_id}/{submission_id}/file/"
    upload_s3_url = f"s3://{bucket_name}/{organization_id}/{submission_id}/file/logs"
    manifest_file = f"s3://{bucket_name}/{organization_id}/{submission_id}/metadata/{manifest_name}"

    stream_file_validator(
        manifest_file,
        None,
        bucket_name,
        file_prefix,
        upload_s3_url,
        file_name_column,
        file_size_column,
        file_md5_column
    )


@flow(name="CRDC Stream File Validator", log_prints=True)
def stream_file_validator(
        manifest_file, #  "s3://<bucket_name>/<file_key>",
        file_url_column,
        # If file urls are not available in the manifest, then bucket name and prefix (folder name) need to be provided
        validation_s3_bucket,
        validation_prefix,
        upload_s3_url, # "s3://<upload_bucket_name>/<upload_file_location>"
        # Column names in the manifest file
        file_name_column = "file_name",
        file_size_column = "file_size",
        file_md5_column = "md5sum"
    ):

    params = Config(
        manifest_file,
        file_name_column,
        file_url_column,
        file_size_column,
        file_md5_column,
        validation_s3_bucket,
        validation_prefix,
        upload_s3_url
    )
    print("Start stream file validating")
    main(params)
    print("Finish stream file validating")

class Config:
    def __init__(
            self,
            manifest_file,
            file_name_column,
            file_url_column,
            file_size_column,
            file_md5_column,
            validation_s3_bucket,
            validation_prefix,
            upload_s3_url
    ):
        
        self.manifest_file = manifest_file
        self.file_name_column = file_name_column
        self.file_url_column = file_url_column
        self.file_size_column = file_size_column
        self.file_md5_column = file_md5_column
        self.validation_s3_bucket = validation_s3_bucket
        self.validation_prefix = validation_prefix
        self.upload_s3_url = upload_s3_url
        self.config_file = None
        

if __name__ == "__main__":
    # create your first deployment
    stream_file_validator.serve(name="local-stream-file-validator-deployment")
    #stream_file_validator()