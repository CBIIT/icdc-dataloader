#!/usr/bin/env python3

import os
from collections import deque
import csv
import re
import datetime
import sys
import platform
import subprocess
import json
import pandas as pd
import datetime
import mgclient
from timeit import default_timer as timer
from bento.common.utils import get_host, DATETIME_FORMAT, reformat_date, get_time_stamp
from memgraph_backup_restore import backup_memgraph_mgconsole
from create_index import create_index, NEO4J, MEMGRAPH

from neo4j import Driver

from icdc_schema import ICDC_Schema, is_parent_pointer
from bento.common.utils import get_logger, NODES_CREATED, RELATIONSHIP_CREATED, UUID, \
    RELATIONSHIP_TYPE, MULTIPLIER, ONE_TO_ONE, DEFAULT_MULTIPLIER, UPSERT_MODE, \
    NEW_MODE, DELETE_MODE, NODES_DELETED, RELATIONSHIP_DELETED, NODES_UPDATED, combined_dict_counters, \
    MISSING_PARENT, NODE_LOADED, get_string_md5

NODE_TYPE = 'type'
PROP_TYPE = 'Type'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
excluded_fields = {NODE_TYPE}
CASE_NODE = 'case'
CASE_ID = 'case_id'
CREATED = 'created'
UPDATED = 'updated'
RELATIONSHIPS = 'relationships'
INT_NODE_CREATED = 'int_node_created'
PROVIDED_PARENTS = 'provided_parents'
RELATIONSHIP_PROPS = 'relationship_properties'
BATCH_SIZE = 1000
OTHER = '__other__'
csv.field_size_limit(sys.maxsize)

def format_as_tuple(node_name, properties):
    """
    Format index info as a tuple
    :param node_name: The name of the node type for the index
    :param properties: The list of node properties being used by the index
    :return: A tuple containing the index node_name followed by the index properties in alphabetical order
    """
    if isinstance(properties, str):
        properties = [properties]
    lst = [node_name] + sorted(properties)
    return tuple(lst)


def backup_neo4j(backup_dir, name, address, log):
    try:
        restore_cmd = 'To restore DB from backup (to remove any changes caused by current data loading, run ' \
                      'following commands:\n '
        restore_cmd += '#' * 160 + '\n'
        neo4j_cmd = 'neo4j-admin restore --from={}/{} --force'.format(backup_dir, name)
        mkdir_cmd = [
            'mkdir',
            '-p',
            backup_dir
        ]
        is_shell = False
        # settings for Windows platforms
        if platform.system() == "Windows":
            mkdir_cmd[2] = os.path.abspath(backup_dir)
            is_shell = True
        cmds = [
            mkdir_cmd,
            [
                'neo4j-admin',
                'backup',
                '--backup-dir={}'.format(backup_dir)
            ]
        ]
        if address in ['localhost', '127.0.0.1']:
            # On Windows, the Neo4j service cannot be accessed through the command line without an absolute path
            # or a custom installation location
            if platform.system() == "Windows":
                restore_cmd += '\tManually stop the Neo4j service\n\t$ {}\n\tManually start the Neo4j service\n'.format(
                    neo4j_cmd)
            else:
                restore_cmd += '\t$ neo4j stop && {} && neo4j start\n'.format(neo4j_cmd)
            for cmd in cmds:
                log.info(cmd)
                subprocess.call(cmd, shell=is_shell)
        else:
            second_cmd = 'sudo systemctl stop neo4j && {} && sudo systemctl start neo4j && exit'.format(neo4j_cmd)
            restore_cmd += '\t$ echo "{}" | ssh -t {} sudo su - neo4j\n'.format(second_cmd, address)
            for cmd in cmds:
                remote_cmd = ['ssh', address, '-o', 'StrictHostKeyChecking=no'] + cmd
                log.info(' '.join(remote_cmd))
                subprocess.call(remote_cmd)
        restore_cmd += '#' * 160
        return restore_cmd
    except Exception as e:
        log.exception(e)
        return False


def check_encoding(file_name):
    utf8 = 'utf-8'
    windows1252 = 'windows-1252'
    try:
        with open(file_name, encoding=utf8) as file:
            for _ in file.readlines():
                pass
        return utf8
    except UnicodeDecodeError:
        return windows1252


# Mask all relationship properties, so they won't participate in property comparison
def get_props_signature(props):
    clean_props = props
    for key in clean_props.keys():
        if '$' in key:
            clean_props[key] = ''
    signature = get_string_md5(str(clean_props))
    return signature


class DataLoader:
    def __init__(self, driver, schema, config=None, memgraph_snapshot_dir=None, plugins=None):
        if plugins is None:
            plugins = []
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.log = get_logger('Data Loader')
        self.driver = driver
        self.database_type = NEO4J
        if config is not None:
            self.database_type = config.database_type
            if config.database_type == MEMGRAPH:
                mg_uri_list = config.neo4j_uri.replace("bolt://", "").split(":")
                mg_host = mg_uri_list[0]
                mg_port = int(mg_uri_list[1])
                mg_connection = mgclient.connect(host=mg_host, port=mg_port, username=config.neo4j_user, password=config.neo4j_password)
                mg_connection.autocommit = True
                self.mg_connection = mg_connection

        self.schema = schema
        self.rel_prop_delimiter = self.schema.rel_prop_delimiter
        self.memgraph_snapshot_dir = memgraph_snapshot_dir
        if plugins:
            for plugin in plugins:
                if not hasattr(plugin, 'create_node'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'should_run'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_created'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_created'):
                    raise ValueError('Invalid Plugin!')
        self.plugins = plugins
        self.nodes_created = 0
        self.nodes_updated = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        self.validation_result_file_key = ""
        self.df_validation_dict = {}
        self.skip_validation_flag = False
        self.cheat_mode = True

    def check_files(self, file_list):
        if not file_list:
            self.log.error('Invalid file list')
            return False
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    self.log.error('File "{}" does not exist'.format(data_file))
                    return False
            return True
 
    def validate_delete_files(self, file_list):
        validation_result = True
        try:
            with self.driver.session() as session:
                for txt in file_list:
                    file_encoding = check_encoding(txt)
                    with open(txt, encoding=file_encoding) as in_file:
                        reader = csv.DictReader(in_file, delimiter='\t')
                        line_number = 1
                        for org_obj in reader:
                            line_number += 1
                            obj = self.cleanup_node(org_obj)
                            id_field = self.schema.get_id_field(obj)
                            if id_field not in obj.keys():
                                self.log.error(f'Line: {line_number}: Required id field {id_field} is missing, validation failed')
                                return False
                            elif obj[id_field] is None:
                                self.log.error(f'Line: {line_number}: Required id field {id_field} is None, validation failed')
                                return False
                            if NODE_TYPE not in obj.keys():
                                self.log.error(f'Line: {line_number}: Required node type field {NODE_TYPE} is missing, validation failed')
                                return True
                            elif obj[NODE_TYPE] is None:
                                self.log.error(f'Line: {line_number}: Required node type field {NODE_TYPE} is None, validation failed')
                                return False
                            node_type = obj.get(NODE_TYPE, None)
                            if not self.node_exists(session, node_type, id_field, obj[id_field]):
                                self.log.error(f'Line: {line_number}: The node to be deleted (:{obj[NODE_TYPE]} {{{id_field}: "{obj[id_field]}"}}) not found in DB!, validation failed')
                                validation_result = False
                            
        except Exception as e:
            self.log.error(e)
            self.log.error("Delete file validation failed, abort the deletion")
            sys.exit(1)
        return validation_result


    def validate_files(self, cheat_mode, loading_mode, file_list, max_violations, temp_folder, verbose):
        if not cheat_mode:
            if loading_mode != DELETE_MODE:
                self.cheat_mode = False
                validation_failed = False
                output_key_invalid = ""
                for txt in file_list:
                    validate_result = self.validate_file(txt, max_violations, verbose)
                    if not validate_result:
                        self.log.error('Validating file "{}" failed!'.format(txt))
                        validation_failed = True
                if validation_failed:
                    if not os.path.exists(temp_folder):
                        os.makedirs(temp_folder)
                    df_validation_result_file_key = os.path.basename(os.path.dirname(file_list[0]))
                    timestamp = get_time_stamp()
                    output_key_invalid = os.path.join(temp_folder, df_validation_result_file_key) + "_" + timestamp + ".xlsx"
                    #df_validation_result.to_csv(output_key_invalid, index=False)
                    writer=pd.ExcelWriter(output_key_invalid, engine='xlsxwriter')
                    for key in self.df_validation_dict.keys():
                        sheet_name_new = key
                        self.df_validation_dict[key].to_excel(writer,sheet_name=sheet_name_new, index=False)
                    writer.close()

                self.validation_result_file_key = output_key_invalid
                return not validation_failed
            elif loading_mode == DELETE_MODE:
                self.log.info("Start validation the delete file.")
                validation_result = self.validate_delete_files(file_list)
                if validation_result:
                    self.log.info("Passed all delete file validation.")
                return validation_result
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')
            return True

    def load(self, file_list, cheat_mode, dry_run, loading_mode, wipe_db, max_violations, temp_folder, verbose,
             split=False, no_backup=True, neo4j_uri=None, backup_folder="/", username=None, password=None):
        if not self.check_files(file_list):
            return False
        start = timer()
        if not self.validate_files(cheat_mode, loading_mode, file_list, max_violations, temp_folder, verbose):
            return False
        if not no_backup and not dry_run:
            if not neo4j_uri:
                self.log.error('No Neo4j URI specified for backup, abort loading!')
                sys.exit(1)
            host = get_host(neo4j_uri)
            if self.database_type == NEO4J:
                backup_name = datetime.datetime.today().strftime(DATETIME_FORMAT)
                restore_cmd = backup_neo4j(backup_folder, backup_name, host, self.log)
                if not restore_cmd:
                    self.log.error('Backup Neo4j failed, abort loading!')
                    sys.exit(1)
            elif self.database_type == MEMGRAPH:
                backup_name = backup_memgraph_mgconsole(backup_folder, self.memgraph_snapshot_dir, username, password, self.log)
                #the memgraph backup function only works if there is a memgraph mgconcole environment(memgraph docker) set up in local
                #if not backup_name:
                #    self.log.error('Backup Memgraph failed, abort loading!')
                #    sys.exit(1)
        if dry_run:
            end = timer()
            self.log.info('Dry run mode, no nodes or relationships loaded.')  # Time in seconds, e.g. 5.38091952400282
            self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
            return {NODES_CREATED: 0, RELATIONSHIP_CREATED: 0}

        self.nodes_created = 0
        self.nodes_updated = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.nodes_stat_updated = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        self.cheat_mode = True
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        # Data updates and schema related updates cannot be performed in the same session so multiple will be created
        # Create new session for schema related updates (index creation)
        try:
            #cursor = self.mg_connection.cursor()
            if self.database_type == NEO4J:
                self.indexes_created = create_index(self.driver, self.schema, self.log, self.database_type)
            elif self.database_type == MEMGRAPH:
                self.indexes_created = create_index(self.mg_connection, self.schema, self.log, self.database_type)
        except Exception as e:
            self.log.exception(e)
            return False
        # Create new session for data related updates
        with self.driver.session() as session:
            # Split Transactions enabled
            if split:
                self._load_all(session, file_list, loading_mode, split, wipe_db)

            # Split Transactions Disabled
            else:
                # Data updates transaction
                tx = session.begin_transaction()
                try:
                    self._load_all(tx, file_list, loading_mode, split, wipe_db)
                    tx.commit()
                except Exception as e:
                    tx.rollback()
                    self.log.exception(e)
                    #return False
                    sys.exit(1)

        # End the timer
        end = timer()

        # Print statistics
        for plugin in self.plugins:
            combined_dict_counters(self.nodes_stat, plugin.nodes_stat)
            combined_dict_counters(self.nodes_stat_updated, plugin.nodes_stat_updated)
            combined_dict_counters(self.relationships_stat, plugin.relationships_stat)
            self.nodes_created += plugin.nodes_created
            self.nodes_updated += plugin.nodes_updated
            self.relationships_created += plugin.relationships_created
        for node in sorted(self.nodes_stat.keys()):
            count = self.nodes_stat[node]
            update_count = self.nodes_stat_updated[node]
            self.log.info('Node: (:{}) loaded: {}'.format(node, count))
            self.log.info('Node: (:{}) updated: {}'.format(node, update_count))
        for rel in sorted(self.relationships_stat.keys()):
            count = self.relationships_stat[rel]
            self.log.info('Relationship: [:{}] loaded: {}'.format(rel, count))
        self.log.info('{} new indexes created!'.format(self.indexes_created))
        self.log.info('{} nodes and {} relationships loaded!'.format(self.nodes_created, self.relationships_created))
        self.log.info('{} nodes and {} relationships deleted!'.format(self.nodes_deleted, self.relationships_deleted))
        self.log.info('{} nodes updated!'.format(self.nodes_updated, self.relationships_deleted))
        self.log.info('Loading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created,
                NODES_DELETED: self.nodes_deleted, RELATIONSHIP_DELETED: self.relationships_deleted, NODES_UPDATED: self.nodes_updated}

    def _load_all(self, tx, file_list, loading_mode, split, wipe_db):
        if wipe_db:
            self.wipe_db(tx, split)
        for txt in file_list:
            self.load_nodes(tx, txt, loading_mode, split)
        if loading_mode != DELETE_MODE:
            for txt in file_list:
                self.load_relationships(tx, txt, loading_mode, split)

    # Remove extra spaces at beginning and end of the keys and values
    @staticmethod
    def cleanup_node(node):
        return {key if not key else key.strip(): value if not value else value.strip() for key, value in node.items()}

    # Cleanup values for Boolean, Int and Float types
    # Add uuid to nodes if one not exists
    # Add parent id(s)
    # Add extra properties for "value with unit" properties
    def prepare_node(self, node, file_name):
        obj = self.cleanup_node(node)
        node_type = obj.get(NODE_TYPE, None)
        # Cleanup values for Boolean, Int and Float types
        if node_type:
            df_validation_result = pd.DataFrame(columns=['File Name', 'Property', 'Value', 'Reason', 'Line Numbers', 'Severity'])
            for key, value in obj.items():
                search_node_type = node_type
                search_key = key
                if is_parent_pointer(key):
                    search_node_type, search_key = key.split('.')
                elif self.schema.is_relationship_property(key):
                    search_node_type, search_key = key.split(self.rel_prop_delimiter)

                key_type = self.schema.get_prop_type(search_node_type, search_key)
                if key_type == 'Boolean':
                    cleaned_value = None
                    if isinstance(value, str):
                        if re.search(r'yes|true', value, re.IGNORECASE):
                            cleaned_value = True
                        elif re.search(r'no|false', value, re.IGNORECASE):
                            cleaned_value = False
                        else:
                            self.log.debug('Unsupported Boolean value: "{}"'.format(value))
                            cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Int':
                    try:
                        if value is None:
                            cleaned_value = None
                        else:
                            cleaned_value = int(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Float':
                    try:
                        if value is None:
                            cleaned_value = None
                        else:
                            cleaned_value = float(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Array':
                    items = self.schema.get_list_values(value)
                    # todo: need to transform items if item type is not string
                    obj[key] = json.dumps(items)
                elif key_type == 'DateTime' or key_type == 'Date':
                    if value is None:
                        cleaned_value = None
                    else:
                        cleaned_value = reformat_date(value)
                    obj[key] = cleaned_value
            obj2 = {}
            for key, value in obj.items():
                obj2[key] = value
                # Add parent id field(s) into node
                if obj[NODE_TYPE] in self.schema.props.save_parent_id and is_parent_pointer(key):
                    header = key.split('.')
                    if len(header) > 2:
                        self.log.warning('Column header "{}" has multiple periods!'.format(key))
                        df_validation_result = self.update_field_validation_result(df_validation_result, file_name, "", "column_header_has_multiple_periods", "warning")
                        if obj[NODE_TYPE] not in self.df_validation_dict.keys():
                            self.df_validation_dict[obj[NODE_TYPE]] = df_validation_result
                        else:
                            self.df_validation_dict[obj[NODE_TYPE]] = pd.concat([self.df_validation_dict[obj[NODE_TYPE]], df_validation_result])
                    field_name = header[1]
                    parent = header[0]
                    combined = '{}_{}'.format(parent, field_name)
                    if field_name in obj:
                        self.log.debug(
                            '"{}" field is in both current node and parent "{}", use {} instead !'.format(key, parent,
                                                                                                        combined))
                        field_name = combined
                    # Add an value for parent id
                    obj2[field_name] = value
                # Add extra properties if any
                for extra_prop_name, extra_value in self.schema.get_extra_props(node_type, key, value).items():
                    obj2[extra_prop_name] = extra_value

            if UUID not in obj2:
                id_field = self.schema.get_id_field(obj2)
                id_value = self.schema.get_id(obj2)
                node_type = obj2.get(NODE_TYPE)
                if node_type:
                    if not id_value:
                        obj2[UUID] = self.schema.get_uuid_for_node(node_type, self.get_signature(obj2))
                    elif id_field != UUID:
                        obj2[UUID] = self.schema.get_uuid_for_node(node_type, id_value)
                else:
                    raise Exception('No "type" column in file')
            return obj2
        elif not self.cheat_mode:
            self.log.error('No "type" column in file')
            #sys.exit(1)
            df_validation_result = pd.DataFrame(columns=['File Name', 'Property', 'Value', 'Reason', 'Line Numbers', 'Severity'])
            df_validation_result = self.update_field_validation_result(df_validation_result, file_name, "", "type_column_missing", "error")
            if OTHER not in self.df_validation_dict.keys():
                self.df_validation_dict[OTHER] = df_validation_result
            else:
                self.df_validation_dict[OTHER] = pd.concat([self.df_validation_dict[OTHER], df_validation_result])
            self.skip_validation_flag = True
            return obj
        else: #if enable cheat mode and bypass the validation
            self.log.error('No "type" column in file, abort loading')
            sys.exit(1)

    def get_signature(self, node):
        result = []
        for key in sorted(node.keys()):
            value = node[key]
            if not is_parent_pointer(key):
                result.append('{}: {}'.format(key, value))
        return '{{ {} }}'.format(', '.join(result))

    # Validate all cases exist in a data (TSV/TXT) file
    def validate_cases_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session() as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.prepare_node(org_obj, file_name)
                    line_num += 1
                    # Validate parent exist
                    if CASE_ID in obj:
                        case_id = obj[CASE_ID]
                        if not self.node_exists(session, CASE_NODE, CASE_ID, case_id):
                            self.log.error(
                                'Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) does not exist!'.format(
                                    line_num, CASE_NODE, CASE_ID, case_id))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                return not validation_failed

    # Validate all parents exist in a data (TSV/TXT) file
    def validate_parents_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session() as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    line_num += 1
                    obj = self.prepare_node(org_obj, file_name)
                    results = self.collect_relationships(obj, session, False, line_num)
                    relationships = results[RELATIONSHIPS]
                    provided_parents = results[PROVIDED_PARENTS]
                    if provided_parents > 0:
                        if len(relationships) == 0:
                            self.log.error('Invalid data at line {}: No parents found!'.format(line_num))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                    else:
                        self.log.info('Line: {} - No parents found'.format(line_num))

        return not validation_failed

    def get_node_properties(self, obj):
        """
        Generate a node with only node properties from input data
        :param obj: input data object (dict), may contain parent pointers, relationship properties etc.
        :return: an object (dict) that only contains properties on this node
        """
        node = {}

        for key, value in obj.items():
            if is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue
            else:
                node[key] = value

        return node

    # Validate the field names
    def validate_field_name(self, file_name):
        df_validation_result = pd.DataFrame(columns=['File Name', 'Property', 'Value', 'Reason', 'Line Numbers', 'Severity'])
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            row = next(reader)
            row = self.cleanup_node(row)
            row_prepare_node = self.prepare_node(row, file_name)
            if self.skip_validation_flag:
                return False
            parent_pointer = []
            for key in row_prepare_node.keys():
                if is_parent_pointer(key):
                    parent_pointer.append(key)
            error_list = []
            parent_error_list = []
            for key in row.keys():
                if key not in parent_pointer:
                    try:
                        if key not in self.schema.get_props_for_node(row['type']) and key != 'type':
                            error_list.append(key)
                    except:
                        error_list.append(key)
                else:
                    try:
                        if key.split('.')[1] not in self.schema.get_props_for_node(key.split('.')[0]):
                            parent_error_list.append(key)
                    except:
                        parent_error_list.append(key)
            if len(error_list) > 0:
                for error_field_name in error_list:
                    self.log.warning('Property: "{}" not found in data model'.format(error_field_name))
                    df_validation_result = self.update_field_validation_result(df_validation_result, file_name, error_field_name, "property_not_found_in_model", "warning")
            if len(parent_error_list) > 0:
                for parent_error_field_name in parent_error_list:
                    self.log.error('Parent pointer: "{}" not found in data model'.format(parent_error_field_name))
                    df_validation_result = self.update_field_validation_result(df_validation_result, file_name, parent_error_field_name, "parent_pointer_not_found_in_model", "error")
                if len(df_validation_result) > 0:
                    if row[NODE_TYPE] not in self.df_validation_dict.keys():
                        self.df_validation_dict[row[NODE_TYPE]] = df_validation_result
                    else:
                        self.df_validation_dict[row[NODE_TYPE]] = pd.concat([self.df_validation_dict[row[NODE_TYPE]], df_validation_result])
                self.log.error('Parent pointer not found in the data model, abort loading!')
                return False
        if len(df_validation_result) > 0:
            if row[NODE_TYPE] not in self.df_validation_dict.keys():
                    self.df_validation_dict[row[NODE_TYPE]] = df_validation_result
            else:
                self.df_validation_dict[row[NODE_TYPE]] = pd.concat([self.df_validation_dict[row[NODE_TYPE]], df_validation_result])
        return True
    # update field validation result
    def update_field_validation_result(self, df_validation_result, file_name, error_field_name, reason, severity):
        tmp_df_validation_result_field = pd.DataFrame()
        tmp_df_validation_result_field['File Name'] = [os.path.basename(file_name)]
        tmp_df_validation_result_field['Property'] = [error_field_name]
        tmp_df_validation_result_field['Reason'] =  [reason]
        tmp_df_validation_result_field['Severity'] = [severity]
        df_validation_result = pd.concat([df_validation_result, tmp_df_validation_result_field])
        return df_validation_result
    # Validate file
    def validate_file(self, file_name, max_violations, verbose):
        self.skip_validation_flag = False
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            violations = 0
            ids = {}
            df_validation_result = pd.DataFrame(columns=['File Name', 'Property', 'Value', 'Reason', 'Line Numbers', 'Severity'])
            field_validation_result = self.validate_field_name(file_name)
            if not field_validation_result:
                return False
            df_invalid = pd.DataFrame(columns=['invalid_properties', 'invalid_values', 'invalid_reason', 'invalid_line_num', 'node_type'])
            df_missing = pd.DataFrame(columns=['missing_properties', 'missing_reason', 'missing_line_num', 'node_type'])
            df_duplicate_id = pd.DataFrame(columns=['duplicate_id', 'duplicate_reason', 'duplicate_id_field', 'duplicate_line_num', 'node_type'])
            duplicate_id = []
            duplicate_reason = []
            duplicate_line_num = []
            duplicate_node_type = []
            duplicate_id_field = []
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                props = self.get_node_properties(obj)
                line_num += 1
                id_field = self.schema.get_id_field(obj)
                node_id = self.schema.get_id(obj)

                if node_id:
                    if node_id in ids:
                        if get_props_signature(props) != ids[node_id]['props']:
                            validation_failed = True
                            self.log.error(
                                f'Invalid data at line {line_num}: duplicate {id_field}: {node_id}, found in line: '
                                f'{", ".join(ids[node_id]["lines"])}')
                            ids[node_id]['lines'].append(str(line_num))
                            duplicate_id.append(node_id)
                            duplicate_reason.append('duplicate_id')
                            duplicate_line_num.append(line_num)
                            duplicate_node_type.append(obj[NODE_TYPE])
                            duplicate_id_field.append(id_field)
                        else:
                            # Same ID exists in same file, but properties are also same, probably it's pointing same
                            # object to multiple parents
                            self.log.debug(
                                f'Duplicated data at line {line_num}: duplicate {id_field}: {node_id}, found in line: '
                                f'{", ".join(ids[node_id]["lines"])}')
                            duplicate_id.append(node_id)
                            duplicate_reason.append('many_to_many')
                            duplicate_line_num.append(line_num)
                            duplicate_node_type.append(obj[NODE_TYPE])
                            duplicate_id_field.append(id_field)
                    else:
                        ids[node_id] = {'props': get_props_signature(props), 'lines': [str(line_num)]}

                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj, verbose)
                try:
                    if len(validate_result['invalid_properties']) > 0:
                        tmp_df_invalid = pd.DataFrame()
                        tmp_df_invalid['invalid_properties'] = validate_result['invalid_properties']
                        tmp_df_invalid['invalid_values'] = validate_result['invalid_values']
                        tmp_df_invalid['invalid_reason'] = validate_result['invalid_reason']
                        tmp_df_invalid['invalid_line_num'] = line_num
                        tmp_df_invalid['node_type'] = obj[NODE_TYPE]
                        df_invalid = pd.concat([df_invalid,tmp_df_invalid])

                except Exception as e:
                    print(e)
                try:
                    if len(validate_result['missing_properties']) > 0:
                        tmp_df_missing = pd.DataFrame()
                        tmp_df_missing['missing_properties'] = validate_result['missing_properties']
                        tmp_df_missing['missing_reason'] = validate_result['missing_reason']
                        tmp_df_missing['missing_line_num'] = line_num
                        tmp_df_missing['node_type'] = obj[NODE_TYPE]
                        df_missing = pd.concat([df_missing, tmp_df_missing])
                except Exception as e:
                    print(e)
                if not validate_result['result'] and not validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.error('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    validation_failed = True
                    violations += 1
                    if violations >= max_violations:
                        #return False, df_validation_dict
                        break
                elif not validate_result['result'] and validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.warning('Invalid data at line {}: "{}"!'.format(line_num, msg))
            # ouput the data vlidation result
            df_duplicate_id['duplicate_id'] = duplicate_id
            df_duplicate_id['duplicate_reason'] = duplicate_reason
            df_duplicate_id['duplicate_line_num'] = duplicate_line_num
            df_duplicate_id['node_type'] = duplicate_node_type
            df_duplicate_id['duplicate_id_field'] = duplicate_id_field
            ''''''
            if len(df_invalid) > 0:
                df_invalid = df_invalid.sort_values(by=['invalid_properties'])
                df_invalid = df_invalid.explode('invalid_line_num').groupby(['invalid_properties', 'invalid_values', 'invalid_reason', 'node_type'])['invalid_line_num'].unique().reset_index()
                tmp_df_validation_result_invalid = pd.DataFrame()
                tmp_df_validation_result_invalid['File Name'] = [os.path.basename(file_name)] * len(df_invalid)
                tmp_df_validation_result_invalid['Property'] = df_invalid['invalid_properties']
                tmp_df_validation_result_invalid['Value'] =  df_invalid['invalid_values']
                tmp_df_validation_result_invalid['Reason'] =  df_invalid['invalid_reason']
                tmp_df_validation_result_invalid['Line Numbers'] = self.convert_line_num_list(list(df_invalid['invalid_line_num']))
                tmp_df_validation_result_invalid['Severity'] = ["error"] * len(df_invalid)
                df_validation_result = pd.concat([df_validation_result, tmp_df_validation_result_invalid])
            if len(df_missing) >0:
                df_missing = df_missing.sort_values(by=['missing_properties'])
                df_missing = df_missing.explode('missing_line_num').groupby(['missing_properties', 'missing_reason', 'node_type'])['missing_line_num'].unique().reset_index()
                tmp_df_validation_result_missing = pd.DataFrame()
                tmp_df_validation_result_missing['File Name'] = [os.path.basename(file_name)] * len(df_missing)
                tmp_df_validation_result_missing['Property'] = df_missing['missing_properties']
                tmp_df_validation_result_missing['Reason'] =  df_missing['missing_reason']
                tmp_df_validation_result_missing['Line Numbers'] = self.convert_line_num_list(list(df_missing['missing_line_num']))
                tmp_df_validation_result_missing['Severity'] = ["error"] * len(df_missing)
                df_validation_result = pd.concat([df_validation_result, tmp_df_validation_result_missing])
            if len(df_duplicate_id) > 0:
                df_duplicate_id = df_duplicate_id.explode('duplicate_line_num').groupby(['duplicate_id', 'duplicate_reason', 'duplicate_id_field', 'node_type'])['duplicate_line_num'].unique().reset_index()
                tmp_df_validation_result_duplicate= pd.DataFrame()
                tmp_df_validation_result_duplicate['File Name'] = [os.path.basename(file_name)] * len(df_duplicate_id)
                tmp_df_validation_result_duplicate['Property'] = df_duplicate_id['duplicate_id_field']
                tmp_df_validation_result_duplicate['Value'] = df_duplicate_id['duplicate_id']
                tmp_df_validation_result_duplicate['Reason'] = df_duplicate_id['duplicate_reason']
                tmp_df_validation_result_duplicate['Line Numbers'] = self.convert_line_num_list(list(df_duplicate_id['duplicate_line_num']))
                tmp_df_validation_result_duplicate['Severity'] = ["error"] * len(df_duplicate_id)
                df_validation_result = pd.concat([df_validation_result, tmp_df_validation_result_duplicate])
            if len(df_validation_result) > 0:
                if obj[NODE_TYPE] not in self.df_validation_dict.keys():
                    self.df_validation_dict[obj[NODE_TYPE]] = df_validation_result
                else:
                    self.df_validation_dict[obj[NODE_TYPE]] = pd.concat([self.df_validation_dict[obj[NODE_TYPE]], df_validation_result])
            return not validation_failed

    def convert_line_num_list(self, line_num_list):
        if len(line_num_list) > 0:
            new_line_num_list = []
            for line_num in line_num_list:
                line_num.sort()
                line_num_str = [str(x) for x in line_num]
                if len(line_num) > 1:
                    new_line_num_list.append(','.join(line_num_str))
                else:
                    new_line_num_list.append(line_num_str[0])
            return new_line_num_list
        else:
            return line_num_list

    def get_new_statement(self, node_type, obj):
        # statement is used to create current node
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('{0}: ${0}'.format(key))

        statement = 'CREATE (:{0} {{ {1} }})'.format(node_type, ' ,'.join(prop_stmts))
        return statement

    def get_upsert_statement(self, node_type, id_field, obj):
        # statement is used to create current node
        statement = ''
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif key == id_field:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('n.{0} = ${0}'.format(key))

        statement += 'MERGE (n:{0} {{ {1}: ${1} }})'.format(node_type, id_field)
        statement += ' ON CREATE ' + 'SET n.{} = datetime(), '.format(CREATED) + ' ,'.join(prop_stmts)
        statement += ' ON MATCH ' + 'SET n.{} = datetime(), '.format(UPDATED) + ' ,'.join(prop_stmts)
        return statement

    # Delete a node and children with no other parents recursively
    def delete_node(self, session, node):
        delete_queue = deque([node])
        node_deleted = 0
        relationship_deleted = 0
        while len(delete_queue) > 0:
            root = delete_queue.popleft()
            delete_queue.extend(self.get_children_with_single_parent(session, root))
            n_deleted, r_deleted = self.delete_single_node(session, root)
            node_deleted += n_deleted
            relationship_deleted += r_deleted
        return node_deleted, relationship_deleted

    # Return children of node without other parents
    def get_children_with_single_parent(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }})<--(m)'.format(node_type, self.schema.get_id_field(node))
        #statement += ' WHERE NOT (n)<--(m)-->() RETURN m'
        statement += ' WHERE NOT EXISTS((n)<--(m)-->()) RETURN m'
        result = session.run(statement, node)
        children = []
        for obj in result:
            children.append(self.get_node_from_result(obj, 'm'))
        return children

    @staticmethod
    def get_node_from_result(record, name):
        node = record.data()[name]
        result = dict(node.items())
        for label in record[0].labels:
            result[NODE_TYPE] = label
            break
        return result

    # Simple delete given node, and it's relationships
    def delete_single_node(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }}) detach delete n'.format(node_type, self.schema.get_id_field(node))
        result = session.run(statement, node)
        nodes_deleted = result.consume().counters.nodes_deleted
        self.nodes_deleted += nodes_deleted
        self.nodes_deleted_stat[node_type] = self.nodes_deleted_stat.get(node_type, 0) + nodes_deleted
        relationship_deleted = result.consume().counters.relationships_deleted
        self.relationships_deleted += relationship_deleted
        return nodes_deleted, relationship_deleted

    # load file
    def load_nodes(self, session, file_name, loading_mode, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        elif loading_mode == DELETE_MODE:
            action_word = 'Deleting'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info('{} nodes from file: {}'.format(action_word, file_name))

        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            nodes_updated = 0
            nodes_deleted = 0
            node_type = 'UNKNOWN'
            relationship_deleted = 0
            line_num = 1
            transaction_counter = 0

            # Use session in one transaction mode
            tx = session
            # Use transactions in split-transactions mode
            if split:
                tx = session.begin_transaction()

            for org_obj in reader:
                line_num += 1
                transaction_counter += 1
                obj = self.prepare_node(org_obj, file_name)
                node_type = obj[NODE_TYPE]
                node_id = self.schema.get_id(obj)
                if not node_id:
                    raise Exception('Line:{}: No ids found!'.format(line_num))
                id_field = self.schema.get_id_field(obj)
                if loading_mode == UPSERT_MODE:
                    statement = self.get_upsert_statement(node_type, id_field, obj)
                elif loading_mode == NEW_MODE:
                    if self.node_exists(tx, node_type, id_field, node_id):
                        raise Exception(
                            'Line: {}: Node (:{} {{ {}: {} }}) exists! Abort loading!'.format(line_num, node_type,
                                                                                              id_field, node_id))
                    else:
                        statement = self.get_new_statement(node_type, obj)
                elif loading_mode == DELETE_MODE:
                    n_deleted, r_deleted = self.delete_node(tx, obj)
                    nodes_deleted += n_deleted
                    relationship_deleted += r_deleted
                else:
                    raise Exception('Wrong loading_mode: {}'.format(loading_mode))

                if loading_mode != DELETE_MODE:
                    result = tx.run(statement, obj)
                    count = result.consume().counters.nodes_created
                    #count the updated nodes
                    update_count = 0
                    if result.consume().counters.nodes_created == 0 and result.consume().counters.nodes_deleted == 0:
                        update_count = 1
                    self.nodes_created += count
                    self.nodes_updated += update_count
                    nodes_created += count
                    nodes_updated += update_count
                    self.nodes_stat[node_type] = self.nodes_stat.get(node_type, 0) + count
                    self.nodes_stat_updated[node_type] = self.nodes_stat_updated.get(node_type, 0) + update_count
                # commit and restart a transaction when batch size reached
                if split and transaction_counter >= BATCH_SIZE:
                    tx.commit()
                    tx = session.begin_transaction()
                    self.log.info(f'{line_num - 1} rows loaded ...')
                    transaction_counter = 0
            # commit last transaction
            if split:
                tx.commit()

            if loading_mode == DELETE_MODE:
                self.log.info('{} node(s) deleted'.format(nodes_deleted))
                self.log.info('{} relationship(s) deleted'.format(relationship_deleted))
            else:
                self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, node_type))
                self.log.info('{} (:{}) node(s) updated'.format(nodes_updated, node_type))


    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{0} {{ {1}: ${1} }}) return m'.format(label, prop)
        result = session.run(statement, {prop: value})
        count = len(result.data())
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def collect_relationships(self, obj, session, create_intermediate_node, line_num):
        node_type = obj[NODE_TYPE]
        relationships = []
        int_node_created = 0
        provided_parents = 0
        relationship_properties = {}
        #print(obj.items())
        for key, value in obj.items():
            if is_parent_pointer(key):
                parent_value_list = self.schema.get_list_values(value)
                for value in parent_value_list:
                    provided_parents += 1
                    other_node, other_id = key.split('.')
                    relationship = self.schema.get_relationship(node_type, other_node)
                    if not isinstance(relationship, dict):
                        self.log.error('Line: {}: Relationship not found!'.format(line_num))
                        raise Exception('Undefined relationship, abort loading!')
                    relationship_name = relationship[RELATIONSHIP_TYPE]
                    multiplier = relationship[MULTIPLIER]
                    if not relationship_name:
                        self.log.error('Line: {}: Relationship not found!'.format(line_num))
                        raise Exception('Undefined relationship, abort loading!')
                    if not self.node_exists(session, other_node, other_id, value):
                        create_parent = False
                        if create_intermediate_node:
                            for plugin in self.plugins:
                                if plugin.should_run(other_node, MISSING_PARENT):
                                    create_parent = True
                                    if plugin.create_node(session, line_num, other_node, value, obj):
                                        int_node_created += 1
                                        relationships.append(
                                            {PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                            RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
                                    else:
                                        self.log.error(
                                            'Line: {}: Could not create {} node automatically!'.format(line_num,
                                                                                                    other_node))
                        else:
                            self.log.warning(
                                'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                                other_id,
                                                                                                value))
                        if not create_parent:
                            self.log.warning(
                                'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                                other_id,
                                                                                                value))
                    else:
                        if multiplier == ONE_TO_ONE and self.parent_already_has_child(session, node_type, obj,
                                                                                    relationship_name, other_node,
                                                                                    other_id, value):
                            self.log.error(
                                'Line: {}: one_to_one relationship failed, parent already has a child!'.format(line_num))
                        else:
                            relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                                RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
            elif self.schema.is_relationship_property(key):
                parent_value_list = self.schema.get_list_values(value)
                for value in parent_value_list:
                    rel_name, prop_name = key.split(self.rel_prop_delimiter)
                    if rel_name not in relationship_properties:
                        relationship_properties[rel_name] = {}
                    relationship_properties[rel_name][prop_name] = value
        return {RELATIONSHIPS: relationships, INT_NODE_CREATED: int_node_created, PROVIDED_PARENTS: provided_parents,
                RELATIONSHIP_PROPS: relationship_properties}

    def parent_already_has_child(self, session, node_type, node, relationship_name, parent_type, parent_id_field,
                                 parent_id):
        statement = 'MATCH (n:{})-[r:{}]->(m:{} {{ {}: $parent_id }}) return n'.format(node_type, relationship_name,
                                                                                       parent_type, parent_id_field)
        result = session.run(statement, {"parent_id": parent_id})
        if result:
            child = result.single()
            if child:
                find_current_node_statement = 'MATCH (n:{0} {{ {1}: ${1} }}) return n'.format(node_type,
                                                                                              self.schema.get_id_field(
                                                                                                  node))
                current_node_result = session.run(find_current_node_statement, node)
                if current_node_result:
                    current_node = current_node_result.single()
                    return child[0].id != current_node[0].id
                else:
                    self.log.error('Could NOT find current node!')

        return False

    # Check if a relationship of same type exists, if so, return a statement which can delete it, otherwise return False
    def has_existing_relationship(self, session, node_type, node, relationship, count_same_parent=False):
        relationship_name = relationship[RELATIONSHIP_TYPE]
        parent_type = relationship[PARENT_TYPE]
        parent_id_field = relationship[PARENT_ID_FIELD]

        base_statement = 'MATCH (n:{0} {{ {1}: ${1} }})-[r:{2}]->(m:{3})'.format(node_type,
                                                                                 self.schema.get_id_field(node),
                                                                                 relationship_name, parent_type)
        statement = base_statement + ' return m.{} AS {}'.format(parent_id_field, PARENT_ID)
        result = session.run(statement, node)
        if result:
            old_parent = result.single()
            if old_parent:
                if count_same_parent:
                    del_statement = base_statement + ' delete r'
                    return del_statement
                else:
                    old_parent_id = old_parent[PARENT_ID]
                    if old_parent_id != relationship[PARENT_ID]:
                        self.log.warning('Old parent is different from new parent, delete relationship to old parent:'
                                         + ' (:{} {{ {}: "{}" }})!'.format(parent_type, parent_id_field, old_parent_id))
                        del_statement = base_statement + ' delete r'
                        return del_statement
        else:
            self.log.error('Remove old relationship failed: Query old relationship failed!')

        return False

    def remove_old_relationship(self, session, node_type, node, relationship):
        del_statement = self.has_existing_relationship(session, node_type, node, relationship)
        if del_statement:
            del_result = session.run(del_statement, node)
            if not del_result:
                self.log.error('Delete old relationship failed!')

    def load_relationships(self, session, file_name, loading_mode, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info('{} relationships from file: {}'.format(action_word, file_name))

        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            relationships_created = {}
            int_nodes_created = 0
            line_num = 1
            transaction_counter = 0

            # Use session in one transaction mode
            tx = session
            # Use transactions in split-transactions mode
            if split:
                tx = session.begin_transaction()
            for org_obj in reader:
                line_num += 1
                transaction_counter += 1
                obj = self.prepare_node(org_obj, file_name)
                node_type = obj[NODE_TYPE]
                results = self.collect_relationships(obj, tx, True, line_num)
                relationships = results[RELATIONSHIPS]
                int_nodes_created += results[INT_NODE_CREATED]
                provided_parents = results[PROVIDED_PARENTS]
                relationship_props = results[RELATIONSHIP_PROPS]
                if provided_parents > 0:
                    if len(relationships) == 0:
                        raise Exception('Line: {}: No parents found, abort loading!'.format(line_num))
                    for relationship in relationships:
                        relationship_name = relationship[RELATIONSHIP_TYPE]
                        multiplier = relationship[MULTIPLIER]
                        parent_node = relationship[PARENT_TYPE]
                        parent_id_field = relationship[PARENT_ID_FIELD]
                        parent_id = relationship[PARENT_ID]
                        properties = relationship_props.get(relationship_name, {})
                        if multiplier in [DEFAULT_MULTIPLIER, ONE_TO_ONE]:
                            if loading_mode == UPSERT_MODE:
                                self.remove_old_relationship(tx, node_type, obj, relationship)
                            elif loading_mode == NEW_MODE:
                                if self.has_existing_relationship(tx, node_type, obj, relationship, True):
                                    raise Exception(
                                        'Line: {}: Relationship already exists, abort loading!'.format(line_num))
                            else:
                                raise Exception('Wrong loading_mode: {}'.format(loading_mode))
                        else:
                            self.log.debug('Multiplier: {}, no action needed!'.format(multiplier))
                        prop_statement = ', '.join(self.get_relationship_prop_statements(properties))
                        statement = 'MATCH (m:{0} {{ {1}: $__parentID__ }})'.format(parent_node, parent_id_field)
                        statement += ' MATCH (n:{0} {{ {1}: ${1} }})'.format(node_type,
                                                                             self.schema.get_id_field(obj))
                        statement += ' MERGE (n)-[r:{}]->(m)'.format(relationship_name)
                        statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                        statement += ', {}'.format(prop_statement) if prop_statement else ''
                        statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)
                        statement += ', {}'.format(prop_statement) if prop_statement else ''

                        result = tx.run(statement, {**obj, "__parentID__": parent_id, **properties})
                        count = result.consume().counters.relationships_created
                        self.relationships_created += count
                        relationship_pattern = '(:{})->[:{}]->(:{})'.format(node_type, relationship_name, parent_node)
                        relationships_created[relationship_pattern] = relationships_created.get(relationship_pattern,
                                                                                                0) + count
                        self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name,
                                                                                                 0) + count
                    for plugin in self.plugins:
                        if plugin.should_run(node_type, NODE_LOADED):
                            if plugin.create_node(session=tx, line_num=line_num, src=obj):
                                int_nodes_created += 1
                # commit and restart a transaction when batch size reached
                if split and transaction_counter >= BATCH_SIZE:
                    tx.commit()
                    tx = session.begin_transaction()
                    self.log.info(f'{line_num - 1} rows loaded ...')
                    transaction_counter = 0

            # commit last transaction
            if split:
                tx.commit()
            if provided_parents == 0:
                    self.log.warning('there is no parent mapping columns in the node {}'.format(node_type))
            for rel, count in relationships_created.items():
                self.log.info('{} {} relationship(s) loaded'.format(count, rel))
            if int_nodes_created > 0:
                self.log.info('{} intermediate node(s) loaded'.format(int_nodes_created))

        return True

    @staticmethod
    def get_relationship_prop_statements(props):
        prop_stmts = []

        for key in props:
            prop_stmts.append('r.{0} = ${0}'.format(key))
        return prop_stmts

    def wipe_db(self, session, split=False):
        if split:
            return self.wipe_db_split(session)
        else:
            cleanup_db = 'MATCH (n) DETACH DELETE n'
            result = session.run(cleanup_db).consume()
            self.nodes_deleted = result.counters.nodes_deleted
            self.relationships_deleted = result.counters.relationships_deleted
            self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
            self.log.info('{} relationships deleted!'.format(self.relationships_deleted))

    def wipe_db_split(self, session):
        while True:
            tx = session.begin_transaction()
            try:
                cleanup_db = f'MATCH (n) WITH n LIMIT {BATCH_SIZE} DETACH DELETE n'
                result = tx.run(cleanup_db).consume()
                tx.commit()
                deleted_nodes = result.counters.nodes_deleted
                self.nodes_deleted += deleted_nodes
                deleted_relationships = result.counters.relationships_deleted
                self.relationships_deleted += deleted_relationships
                self.log.info(f'{deleted_nodes} nodes deleted...')
                self.log.info(f'{deleted_relationships} relationships deleted...')
                if deleted_nodes == 0 and deleted_relationships == 0:
                    break
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                raise e
        self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
        self.log.info('{} relationships deleted!'.format(self.relationships_deleted))