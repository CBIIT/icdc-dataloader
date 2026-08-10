import csv

# Adjust this import based on where check_encoding currently lives
from utils import check_encoding


class PreprocessedFileReader:
    def __init__(self, file_name, logger, delimiter='\t', log_summary=True):
        self.file_name = file_name
        self.logger = logger
        self.delimiter = delimiter
        self.empty_columns_dropped = 0
        self.empty_rows_dropped = 0
        self._file_handle = None
        self._reader = None
        self._kept_indexes = []
        self._cleaned_headers = []
        self._line_num = 1
        self.log_summary = log_summary

    def __enter__(self):
        file_encoding = check_encoding(self.file_name)

        self._file_handle = open(
            self.file_name,
            encoding=file_encoding,
            newline=''
        )

        self._reader = csv.reader(
            self._file_handle,
            delimiter=self.delimiter
        )

        try:
            headers = next(self._reader)
        except StopIteration:
            headers = []

        for idx, header in enumerate(headers):
            header_name = '' if header is None else str(header).strip()

            if not header_name:
                self.empty_columns_dropped += 1
                continue

            self._kept_indexes.append(idx)
            self._cleaned_headers.append(header_name)

        return self

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            raw_row = next(self._reader)
            self._line_num += 1

            row = {}
            has_value = False

            for idx, header in zip(
                self._kept_indexes,
                self._cleaned_headers
            ):
                value = raw_row[idx] if idx < len(raw_row) else ''
                value = '' if value is None else str(value).strip()

                if value != '':
                    has_value = True

                row[header] = value

            if not has_value:
                self.empty_rows_dropped += 1
                continue

            return self._line_num, row

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file_handle:
            self._file_handle.close()

        if self.logger and self.log_summary:
            self.logger.info(
                'Preprocessing file "{}": dropped {} empty column '
                'header(s) and {} empty row(s).'.format(
                    self.file_name,
                    self.empty_columns_dropped,
                    self.empty_rows_dropped
                )
            )

        return False
    