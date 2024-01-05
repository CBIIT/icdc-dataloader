from prefect import flow
from stream_file_validator import main


@flow(name="CRDC Stream File Validator", log_prints=True)
def stream_file_validator(
        manifest_file = "s3://<bucket_name>/<file_key>",
        # Column names in the manifest file
        file_name_column = "file_name",
        file_url_column = "file_location",
        file_size_column = "file_size",
        file_md5_column = "md5sum",
        # If file urls are not available in the manifest, then bucket name and prefix (folder name) need to be provided
        validation_s3_bucket = "bucket",
        validation_prefix = "prefix",
        upload_s3_url = "s3://<upload_bucket_name>/<upload_file_location>"
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