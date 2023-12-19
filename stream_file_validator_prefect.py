from prefect import flow
from stream_file_validator import main


@flow(name="CRDC Stream File Validator", log_prints=True)
def stream_file_validator(
        manifest_folder = "manifest_file_folder",
        # Column names in the manifest file
        file_name = "file_name",
        file_url = "file_location",
        file_size = "file_size",
        file_md5 = "md5sum",
        # If file urls are not available in the manifest, then bucket name and prefix (folder name) need to be provided
        s3_validation_bucket_list = ["s3_bucket_1"],
        s3_validation_prefix_list = ["s3_prefect_1"],
        download_from_s3 = True,
        s3_download_bucket = "s3_download_bucket",
        s3_download_prefix = "s3_download_prefix",
        # Output file location, defualt is tmp/
        output_folder = "tmp"
    ):

    params = Config(
        manifest_folder,
        file_name,
        file_url,
        file_size,
        file_md5,
        s3_validation_bucket_list,
        s3_validation_prefix_list,
        download_from_s3,
        s3_download_bucket,
        s3_download_prefix,
        output_folder
    )
    print("Start stream file validating")
    main(params)
    print("Finish stream file validating")

class Config:
    def __init__(
            self,
            manifest_folder,
            file_name,
            file_url,
            file_size,
            file_md5,
            s3_validation_bucket_list,
            s3_validation_prefix_list,
            download_from_s3,
            s3_download_bucket,
            s3_download_prefix,
            output_folder
    ):
        
        self.manifest_folder = manifest_folder
        self.file_name = file_name
        self.file_url = file_url
        self.file_size = file_size
        self.file_md5 = file_md5
        self.s3_validation_bucket_list = s3_validation_bucket_list
        self.s3_validation_prefix_list = s3_validation_prefix_list
        self.output_folder = output_folder
        self.download_from_s3 = download_from_s3
        self.s3_download_bucket = s3_download_bucket
        self.s3_download_prefix = s3_download_prefix
        self.config_file = None


if __name__ == "__main__":
    # create your first deployment
    stream_file_validator.serve(name="local-stream-file-validator-deployment")
    #stream_file_validator()