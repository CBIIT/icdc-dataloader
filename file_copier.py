#!/usr/bin/env python3

from collections import deque
import csv
import json
import os


from bento.common.sqs import Queue, VisibilityExtender
from bento.common.utils import get_logger, get_uuid, LOG_PREFIX, UUID, get_time_stamp, removeTrailingSlash
from glioma import Glioma
from copier import Copier
from file_copier_config import MASTER_MODE, SLAVE_MODE, SOLO_MODE, Config

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_Loader'

""""
This script copies (stream in memory) files from an URL to specified S3 bucket

Inputs:
  pre-manifest: TSV file that contains all information of original files
  target bucket: 
"""


class FileLoader:

    DEFAULT_ACL = "['Open']"
    GUID = 'GUID'
    MD5 = 'md5'
    SIZE = 'size'
    ACL = 'acl'
    URL = 'url'
    MANIFEST_FIELDS = [GUID, MD5, SIZE, ACL, URL]

    FILE_SIZE = "file_size"
    MD5_SUM = 'md5sum'
    FILE_STAT = 'file_status'
    FILE_LOC = 'file_locations'
    FILE_FORMAT = 'file_format'
    DATA_FIELDS = [UUID, FILE_SIZE, MD5_SUM, FILE_STAT, FILE_LOC, FILE_FORMAT, ACL]

    DEFAULT_STAT = 'uploaded'
    INDEXD_GUID_PREFIX = 'dg.4DFC/'
    INDEXD_MANIFEST_EXT = '.tsv'
    VISIBILITY_TIMEOUT = 30

    # keys in job dict
    TTL = 'ttl'
    INFO = 'file_info'
    LINE = 'line_num'
    OVERWRITE = 'overwrite'
    DRY_RUN = 'dry_run'
    BUCKET = 'bucket'
    PREFIX = 'prefix'

    def __init__(self, mode, adapter, domain=None, bucket=None, prefix=None, pre_manifest=None, first=1, count=-1, job_queue=None, result_queue=None, retry=3, overwrite=False, dryrun=False):
        """"

        :param bucket: string type
        :param pre_manifest: string type, holds path to pre-manifest
        :param first: first file of files to process, file 1 is in line 2 of pre-manifest
        :param count: number of files to process
        :param adapter: any object that has following methods/properties defined in adapter_attrs

        """
        if mode != MASTER_MODE and mode != SLAVE_MODE and mode != SOLO_MODE:
            raise ValueError(f'Invalid loading mode: {mode}')
        self.mode = mode
        if mode != SOLO_MODE:
            if not job_queue:
                raise ValueError(f'Job queue name is required in {self.mode} mode!')
            self.job_queue_name = job_queue
            self.job_queue = Queue(job_queue)
            if not result_queue:
                raise ValueError(f'Result queue name is required in {self.mode} mode!')
            self.result_queue_name = result_queue
            self.result_queue = Queue(result_queue)

        if self.mode != SLAVE_MODE:
            if not bucket:
                raise ValueError('Empty destination bucket name')
            self.bucket_name = bucket

            if prefix and isinstance(prefix, str):
                self.prefix = removeTrailingSlash(prefix)
            else:
                raise ValueError(f'Invalid prefix: "{prefix}"')

            if not pre_manifest or not os.path.isfile(pre_manifest):
                raise ValueError(f'Pre-manifest: "{pre_manifest}" dosen\'t exist')
            self.pre_manifest = pre_manifest

            if not domain:
                raise ValueError(f'Empty domain!')
            self.domain = domain

        if not hasattr(adapter, 'filter_fields'):
            raise TypeError(f'Adapter does not have a "filter_fields" method')
        self.adapter = adapter
        self.copier = None

        if not first > 0 or count == 0:
            raise ValueError(f'Invalid first ({first}) or count ({count})')
        self.skip = first - 1
        self.count = count

        if not isinstance(retry, int) and retry > 0:
            raise ValueError(f'Invalid retry value: {retry}')
        self.retry = retry
        if not isinstance(overwrite, bool):
            raise TypeError(f'Invalid overwrite value: {overwrite}')
        self.overwrite = overwrite
        if not isinstance(dryrun, bool):
            raise TypeError(f'Invalid dryrun value: {dryrun}')
        self.dryrun = dryrun

        self.log = get_logger('FileLoader')

        # Statistics
        self.files_processed = 0
        self.files_skipped = 0
        self.files_failed = 0

    def get_indexd_manifest_name(self, file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, _ = os.path.splitext(base_name)
        new_name = '{}_indexd{}'.format(name, self.INDEXD_MANIFEST_EXT)
        return os.path.join(folder, new_name)

    @staticmethod
    def get_s3_location(bucket, key):
        return "s3://{}/{}".format(bucket, key)

    @staticmethod
    def get_neo4j_manifest_name(file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, ext = os.path.splitext(base_name)
        new_name = '{}_neo4j{}'.format(name, ext)
        return os.path.join(folder, new_name)

    def populate_indexd_record(self, record, result):
        record[self.SIZE] = result[Copier.SIZE]
        record[self.MD5] = result[Copier.MD5]
        record[self.GUID] = '{}{}'.format(self.INDEXD_GUID_PREFIX, get_uuid(self.domain, "file", record[self.MD5]))
        record[self.ACL] = self.DEFAULT_ACL
        record[self.URL] = self.get_s3_location(self.bucket_name, result[Copier.KEY])
        return record

    def populate_neo4j_record(self, record, result):
        record[self.FILE_SIZE] = result[Copier.SIZE]
        record[self.FILE_LOC] = self.get_s3_location(self.bucket_name, result[Copier.KEY])
        file_name = result[Copier.NAME]
        record[self.MD5_SUM] = result[Copier.MD5]
        record[self.FILE_FORMAT] = (os.path.splitext(file_name)[1]).split('.')[1].lower()
        record[UUID] = get_uuid(self.domain, "file", record[self.MD5_SUM])
        record[self.FILE_STAT] = self.DEFAULT_STAT
        record[self.ACL] = self.DEFAULT_ACL
        return record

    def _read_pre_manifest(self, overwrite, retry, dryrun):
        files = []
        with open(self.pre_manifest) as pre_m:
            reader = csv.DictReader(pre_m, delimiter='\t')
            self.field_names = reader.fieldnames
            for i in range(self.skip):
                next(reader)
                self.files_skipped += 1

            line_num = self.files_skipped + 1
            for info in reader:
                self.files_processed += 1
                line_num += 1
                files.append({self.LINE: line_num,
                              self.TTL: retry,
                              self.OVERWRITE: overwrite,
                              self.DRY_RUN: dryrun,
                              self.INFO: info,
                              self.BUCKET: self.bucket_name,
                              self.PREFIX: self.prefix
                              })
                if self.files_processed >= self.count > 0:
                    break
        return files

    # Use this method in solo mode
    def copy_all(self, overwrite, retry, dryrun):
        """
          Read file information from pre-manifest and copy them all to destination bucket
          :param overwrite: overwrite same file at destination
          :param retry:
          :param dryrun:
          :return:
        """
        if self.mode != SOLO_MODE:
            self.log.critical(f'Function only works in {SOLO_MODE} mode!')
            return False
        self.copier = Copier(self.bucket_name, self.prefix, self.adapter)

        file_queue = deque(self._read_pre_manifest(overwrite, retry, dryrun))

        indexd_manifest = self.get_indexd_manifest_name(self.pre_manifest)
        neo4j_manifest = self.get_neo4j_manifest_name(self.pre_manifest)

        with open(indexd_manifest, 'w', newline='\n') as indexd_f:
            indexd_writer = csv.DictWriter(indexd_f, delimiter='\t', fieldnames=self.MANIFEST_FIELDS)
            indexd_writer.writeheader()
            with open(neo4j_manifest, 'w', newline='\n') as neo4j_f:
                fieldnames = self.adapter.filter_fields(self.field_names)
                fieldnames += self.DATA_FIELDS
                neo4j_writer = csv.DictWriter(neo4j_f, delimiter='\t', fieldnames=fieldnames)
                neo4j_writer.writeheader()

                while file_queue:
                    job = file_queue.popleft()
                    job[self.TTL] -= 1
                    file_info = job[self.INFO]
                    try:
                        result = self.copier.stream_file(file_info, overwrite, dryrun)
                        if result[Copier.STATUS]:
                            indexd_record = {}
                            self.populate_indexd_record(indexd_record, result)
                            indexd_writer.writerow(indexd_record)
                            neo4j_record = result[Copier.FIELDS]
                            self.populate_neo4j_record(neo4j_record, result)
                            neo4j_writer.writerow(neo4j_record)
                        else:
                            self._deal_with_failed_file(job, file_queue)
                    except Exception as e:
                        self.log.debug(e)
                        self._deal_with_failed_file(job, file_queue)

                if self.files_skipped > 0:
                    self.log.info(f'Files skipped: {self.files_skipped}')
                self.log.info(f'Files processed: {self.files_processed}')
                self.log.info(f'Files not found: {len(self.copier.files_not_found)}')
                self.log.info(f'Files copied: {self.copier.files_copied}')
                self.log.info(f'Files exist at destination: {self.copier.files_exist_at_dest}')
                self.log.info(f'Files failed: {self.files_failed}')

    def _deal_with_failed_file(self, job, queue):
        if job[self.TTL] > 0:
            self.log.error(f'Line: {job[self.LINE]} - Copying file FAILED! Retry left: {job[self.TTL]}')
            queue.append(job)
        else:
            self.log.critical(f'Copying file failure exceeded maximum retry times, abort!')
            self.files_failed += 1

    # Use this method in master mode
    def process_all(self, overwrite, retry, dryrun):
        """
        Read file information from pre-manifest and push jobs into job queue
        Listen on result queue for loading result
        :param overwrite: overwrite same file at destination
        :param retry:
        :param dryrun:
        :return:
        """
        if self.mode != MASTER_MODE:
            self.log.critical(f'Function only works in {MASTER_MODE} mode!')
            return False

        try:
            files = self._read_pre_manifest(overwrite, retry, dryrun)
            count = 0
            for job in files:
                if dryrun:
                    self.log.info(f'Dry run mode, jobs will be sent to queue but files won\'t be copied!')
                else:
                    self.log.info(f'Line {job[self.LINE]}: file info sent to queue: {self.job_queue_name}')
                self.job_queue.sendMsgToQueue(job, f'{job[self.LINE]}_{get_time_stamp()}')
                count += 1
            self.log.info(f'Files sent to queue: {count}')
            self.read_result(count)

        except Exception as e:
            self.log.debug(e)
            self.log.critical(f'Process files FAILED! Check debug log for detailed information.')


    # read result from result queue - master mode
    def read_result(self, num_files):
        if self.mode != MASTER_MODE:
            self.log.critical(f'Function only works in {MASTER_MODE} mode!')
            return False
        indexd_manifest = self.get_indexd_manifest_name(self.pre_manifest)
        neo4j_manifest = self.get_neo4j_manifest_name(self.pre_manifest)

        with open(indexd_manifest, 'w', newline='\n') as indexd_f:
            indexd_writer = csv.DictWriter(indexd_f, delimiter='\t', fieldnames=self.MANIFEST_FIELDS)
            indexd_writer.writeheader()
            with open(neo4j_manifest, 'w', newline='\n') as neo4j_f:
                fieldnames = self.adapter.filter_fields(self.field_names)
                fieldnames += self.DATA_FIELDS
                neo4j_writer = csv.DictWriter(neo4j_f, delimiter='\t', fieldnames=fieldnames)
                neo4j_writer.writeheader()

                count = 0
                while count < num_files:
                    self.log.info(f'Waiting for results on queue: {self.result_queue_name}, {num_files - count} files pending')
                    for msg in self.result_queue.receiveMsgs(self.VISIBILITY_TIMEOUT):
                        self.log.info(f'Received a result!')
                        extender = None
                        try:
                            result = json.loads(msg.body)
                            # Make sure result is in correct format
                            if (result and
                                Copier.STATUS in result and
                                Copier.MD5 in result and
                                Copier.NAME in result and
                                Copier.KEY in result and
                                Copier.FIELDS in  result
                            ):
                                extender = VisibilityExtender(msg, self.VISIBILITY_TIMEOUT)

                                if result[Copier.STATUS]:
                                    indexd_record = {}
                                    self.populate_indexd_record(indexd_record, result)
                                    indexd_writer.writerow(indexd_record)
                                    neo4j_record = result[Copier.FIELDS]
                                    self.populate_neo4j_record(neo4j_record, result)
                                    neo4j_writer.writerow(neo4j_record)
                                else:
                                    self.log.error(f'Copy file {result[Copier.NAME]} FAILED!')
                                    self.files_failed += 1

                                extender.stop()
                                extender = None
                                count += 1
                                self.log.info(f'{count} of {num_files} files finished!')
                                msg.delete()
                            else:
                                self.log.error(f'Wrong message type!')
                                self.log.error(result)
                                msg.delete()

                        except Exception as e:
                            self.log.dubug(e)
                            self.log.critical(f'Something wrong happened while processing file! Check debug log for details.')

                        finally:
                            if extender:
                                extender.stop()
                                extender = None

        self.log.info(f'All {num_files} files finished!')


    # Use this method in slave mode
    def start_work(self, local_dryrun):
        if self.mode != SLAVE_MODE:
            self.log.critical(f'Function only works in {SLAVE_MODE} mode!')
            return False

        while True:
            try:
                self.log.info(f'Waiting for jobs on queue: {self.job_queue_name}, {self.files_processed} files have been processed so far')
                for msg in self.job_queue.receiveMsgs(self.VISIBILITY_TIMEOUT):
                    self.log.info(f'Received a job!')
                    extender = None
                    data = None
                    try:
                        data = json.loads(msg.body)
                        self.log.debug(data)
                        # Make sure job is in correct format
                        if (self.BUCKET in data and
                            self.INFO in data and
                            self.TTL in data and
                            self.OVERWRITE in data and
                            self.PREFIX in data and
                            self.DRY_RUN in data):
                            dryrun = data[self.DRY_RUN]

                            extender = VisibilityExtender(msg, self.VISIBILITY_TIMEOUT)
                            bucket_name = data[self.BUCKET]
                            prefix = data[self.PREFIX]
                            if not self.copier:
                                self.copier = Copier(bucket_name, prefix, self.adapter)
                            else:
                                self.copier.set_bucket(bucket_name)

                            result = self.copier.stream_file(data[self.INFO], data[self.OVERWRITE], dryrun or local_dryrun)

                            if result[Copier.STATUS]:
                                self.result_queue.sendMsgToQueue(result, f'{result[Copier.NAME]}_{get_time_stamp()}')
                            else:
                                self._deal_with_failed_file_sqs(data)

                            extender.stop()
                            extender = None
                            self.files_processed += 1
                            self.log.info(f'Copying file finished!')
                            msg.delete()
                        else:
                            self.log.error(f'Wrong message type!')
                            self.log.error(data)
                            msg.delete()

                    except Exception as e:
                        self.log.dubug(e)
                        self.log.critical(f'Something wrong happened while processing file! Check debug log for details.')
                        if data:
                            self._deal_with_failed_file_sqs(data)

                    finally:
                        if extender:
                            extender.stop()
                            extender = None

            except KeyboardInterrupt:
                self.log.info('Good bye!')
                return

    def _deal_with_failed_file_sqs(self, job):
        self.log.info(f'Copy file FAILED, {job[self.TTL] - 1} retry left!')
        job[self.TTL] -= 1
        self.job_queue.sendMsgToQueue(job, f'{job[self.LINE]}_{job[self.TTL]}')

    def run(self, overwrite=None, retry=None, dryrun=None):
        overwrite = overwrite if overwrite is not None else self.overwrite
        retry = retry if retry is not None else self.retry
        dryrun = dryrun if dryrun is not None else self.dryrun

        if self.mode == SOLO_MODE:
            self.copy_all(overwrite, retry, dryrun)
        elif self.mode == MASTER_MODE:
            self.process_all(overwrite, retry, dryrun)
        elif self.mode == SLAVE_MODE:
            self.start_work(dryrun)



def main():
    config = Config()
    if not config.validate():
        return

    loader = FileLoader(adapter=Glioma(), **config.data)
    loader.run()

if __name__ == '__main__':
    main()
