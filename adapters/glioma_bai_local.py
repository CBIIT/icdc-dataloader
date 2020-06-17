from .local_adapter import BentoLocal

class GliomaBaiLocal(BentoLocal):
    md5_field = 'original_md5sum'
    size_field = 'original_file_size'
    cleanup_fields = [md5_field, size_field]