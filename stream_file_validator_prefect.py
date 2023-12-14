from prefect import flow
from stream_file_validator import main


@flow(name="CRDC Stream File Validator", log_prints=True)
def stream_file_validator(
        manifest_file = "input_file_location",
        file_name = "file_name",
        file_url = "file_location",
        file_size = "file_size",
        file_md5 = "md5sum",
        bucket = "bruce-file-copier",
        prefix = "test",
        output_file_location = "tmp"
    ):

    params = Config(
        manifest_file,
        file_name,
        file_url,
        file_size,
        file_md5,
        bucket,
        prefix,
        output_file_location
    )
    print("Start stream file validating")
    main(params)
    print("Finish stream file validating")

class Config:
    def __init__(
            self,
            manifest_file,
            file_name,
            file_url,
            file_size,
            file_md5,
            bucket,
            prefix,
            output_file_location
    ):
        
        self.manifest_file = manifest_file
        self.file_name = file_name
        self.file_url = file_url
        self.file_size = file_size
        self.file_md5 = file_md5
        self.bucket = bucket
        self.prefix = prefix
        self.output_file_location = output_file_location
        self.config_file = None


if __name__ == "__main__":
    # create your first deployment
    stream_file_validator.serve(name="local-stream-file-validator-deployment")
    #stream_file_validator()