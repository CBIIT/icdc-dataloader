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
import time
from timeit import default_timer as timer
from bento.common.utils import get_host, DATETIME_FORMAT, reformat_date

from neo4j import Driver
import pandas as pd

from icdc_schema import ICDC_Schema, is_parent_pointer, get_list_values
from bento.common.utils import get_logger, NODES_CREATED, RELATIONSHIP_CREATED, UUID, \
    RELATIONSHIP_TYPE, MULTIPLIER, ONE_TO_ONE, DEFAULT_MULTIPLIER, UPSERT_MODE, \
    NEW_MODE, DELETE_MODE, NODES_DELETED, RELATIONSHIP_DELETED, combined_dict_counters, \
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


def get_btree_indexes(session):
    """
    Queries the database to get all existing indexes
    :param session: the current neo4j transaction session
    :return: A set of tuples representing all existing indexes in the database
    """
    command = "SHOW INDEXES"
    result = session.run(command)
    indexes = set()
    for r in result:
        if r["type"] == "BTREE":
            indexes.add(format_as_tuple(r["labelsOrTypes"][0], r["properties"]))
    return indexes

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
    def __init__(self, driver, schema, database_name, plugins=None):
        if plugins is None:
            plugins = []
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.log = get_logger('Data Loader')
        self.driver = driver
        self.schema = schema
        self.db_name = database_name
        self.rel_prop_delimiter = self.schema.rel_prop_delimiter

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
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        
    def get_schema_data(self, tx, query):
        result = tx.run(query)
        data_list = [i for i in result.data()]
        return data_list
    
    def get_data(self, tx, query, obj_data, batch_size):
        
        #query = "CALL apoc.periodic.iterate( " + query + """ {batchSize: """ + str(batch_size) + """, parallel:true})"""
        result = tx.run(query, batch=obj_data)
        return result.data()
        
    def get_schema_indexes(self):
        self.node_keys_dict = dict()
    
        with self.driver.session(database = self.db_name) as session:
            query = 'SHOW INDEXES'
        
            records = session.execute_read(self.get_schema_data, query)
            for record in records:
                if(record['labelsOrTypes'] != None):
                    self.node_keys_dict[record['labelsOrTypes'][0]] = {'Primary ID': record['properties'][0], 'Node_Index_DF':  pd.DataFrame(columns=['Node_ID', 'Primary_Key_Value'])}
    
            for curr_node in self.node_keys_dict:
                #index_df = pd.DataFrame(columns = ['Node_ID', 'Primary_Key_Value'])
                query = f"MATCH (n:{curr_node}) RETURN ID(n) as Node_ID, n.{self.node_keys_dict[curr_node]['Primary ID']} as Primary_Key_Value"

                records = session.execute_read(self.get_schema_data, query)
                if len(records) > 0:
                    data_res = pd.DataFrame(records)
                    self.node_keys_dict[curr_node]['Node_Index_DF'] = data_res

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

    def validate_files(self, cheat_mode, file_list, max_violations):
        if not cheat_mode:
            validation_failed = False
            for txt in file_list:
                if not self.validate_file(txt, max_violations):
                    self.log.error('Validating file "{}" failed!'.format(txt))
                    validation_failed = True
            return not validation_failed
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')
            return True

    def load(self, file_list, cheat_mode, dry_run, loading_mode, wipe_db, max_violations,
             split=False, no_backup=True, backup_folder="/", neo4j_uri=None):
        if not self.check_files(file_list):
            return False
        start = timer()
        if not self.validate_files(cheat_mode, file_list, max_violations):
            return False
        if not no_backup and not dry_run:
            if not neo4j_uri:
                self.log.error('No Neo4j URI specified for backup, abort loading!')
                sys.exit(1)
            backup_name = datetime.datetime.today().strftime(DATETIME_FORMAT)
            host = get_host(neo4j_uri)
            restore_cmd = backup_neo4j(backup_folder, backup_name, host, self.log)
            if not restore_cmd:
                self.log.error('Backup Neo4j failed, abort loading!')
                sys.exit(1)
        if dry_run:
            end = timer()
            self.log.info('Dry run mode, no nodes or relationships loaded.')  # Time in seconds, e.g. 5.38091952400282
            self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
            return {NODES_CREATED: 0, RELATIONSHIP_CREATED: 0}

        self.nodes_created = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        # Data updates and schema related updates cannot be performed in the same session so multiple will be created
        # Create new session for schema related updates (index creation)
        
        self.log.info("  ")
        self.log.info(f"Database name being used is: {self.db_name} ")
        self.log.info("  ")
        
        with self.driver.session(database = self.db_name) as session:
            tx = session.begin_transaction()
            try:
                self.create_indexes(tx)
                tx.commit()
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                return False

        # Create new session for data related updates
        with self.driver.session(database = self.db_name) as session:
            # Split Transactions enabled
            if split:
                self._load_all(session, file_list, loading_mode, split, wipe_db)

            # Split Transactions Disabled
            else:
                # Data updates transaction
                tx = session.begin_transaction()
                try:
                    self._load_all(session, tx, file_list, loading_mode, split, wipe_db)
                    #tx.commit()
                except Exception as e:
                    tx.rollback()
                    self.log.exception(e)
                    return False

        if session:
            session.close()

        # End the timer
        end = timer()

        # Print statistics
        for plugin in self.plugins:
            combined_dict_counters(self.nodes_stat, plugin.nodes_stat)
            combined_dict_counters(self.relationships_stat, plugin.relationships_stat)
            self.nodes_created += plugin.nodes_created
            self.relationships_created += plugin.relationships_created
        #for node in sorted(self.nodes_stat.keys()):
        #    count = self.nodes_stat[node]
        #    self.log.info('Node: (:{}) loaded: {}'.format(node, count))
        #for rel in sorted(self.relationships_stat.keys()):
        #    count = self.relationships_stat[rel]
        #    self.log.info('Relationship: [:{}] loaded: {}'.format(rel, count))
        #self.log.info('{} new indexes created!'.format(self.indexes_created))
        #self.log.info('{} nodes and {} relationships loaded!'.format(self.nodes_created, self.relationships_created))
        #self.log.info('{} nodes and {} relationships deleted!'.format(self.nodes_deleted, self.relationships_deleted))
        #self.log.info('Loading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created,
               NODES_DELETED: self.nodes_deleted, RELATIONSHIP_DELETED: self.relationships_deleted}


    def _load_all(self,session,  tx, file_list, loading_mode, split, wipe_db):
        if wipe_db:
            self.wipe_db(session)   #deletes all node in db
            
        self.load_passed =0
        self.load_failed = 0
        self.relationship_passed =0
        self.relationship_failed = 0
        
        ## call create index after the wipe database (if was called)
        start = timer()
        self.log.info(' ')
        self.get_schema_indexes()   #create a dictionary that maps the current neo4j database as defined by self.db_name
        end = timer()
        self.log.info('Creating Index Dictionary took: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282


        self.log.info(' ')
        processed_files = []                
        load_start_time = time.perf_counter()
        for txt in file_list:
            if loading_mode != DELETE_MODE:
                new_nodes, updated_nodes, all_obj_list = self.load_nodes(session, tx, txt, loading_mode, wipe_db, split)
                processed_files.append(all_obj_list)
        
        self.log.info(f"Number of Nodes Created / Updated: {self.load_passed}, Nodes Failed: {self.load_failed}")
        self.log.info(f"Total Loading time for all nodes: {time.perf_counter()-load_start_time:.4f} seconds")
        
        self.log.info(' ')
        self.log.info('updating schema dictionary  for new nodes created')
        self.get_schema_indexes()   #create a dictionary that maps the current neo4j database as defined by self.db_name
        self.log.info('schema dictionary has been refreshed')  # Time in seconds, e.g. 5.38091952400282
        self.log.info(' ')
        
        #for txt in file_list:
        rel_start_time = time.perf_counter()
        batch_size = 1000
        
        for txt in processed_files:
            if loading_mode != DELETE_MODE:
                nodes_done = 0
                while nodes_done < len(txt):
                    #batch_time = time.time()
                    if len(txt) <= batch_size:
                        start_node = 0
                        end_node = len(txt)
                    elif (nodes_done+batch_size) > len(txt):
                        start_node = nodes_done
                        end_node = len(txt)
                    else: 
                        start_node = nodes_done
                        end_node = nodes_done + batch_size
                    data_to_work = txt[start_node: end_node]
                
                    #print(data_to_work)

                    self.load_relationships(session, data_to_work, loading_mode, split)
                    nodes_done += len(data_to_work)
#                    print(f"Total Elapsed time for relationships: {time.perf_counter()-batch_time:.4f} seconds")

        self.log.info(f"Number of Relationships Created / Updated: {self.relationship_passed}, Nodes Failed: {self.relationship_failed}")
        self.log.info(f"Total time to make all relationships {time.perf_counter()-rel_start_time:.4f} seconds")

    # Remove extra spaces at beginning and end of the keys and values
    @staticmethod
    def cleanup_node(node):
        return {key if not key else key.strip(): value if not value else value.strip() if isinstance(value,str) else value for key, value in node.items()}
        #return {key if not key else key.strip(): value if not value else value.strip() for key, value in node.items()}

    # Cleanup values for Boolean, Int and Float types
    # Add uuid to nodes if one not exists
    # Add parent id(s)
    # Add extra properties for "value with unit" properties
    def prepare_node(self, node):
        obj = self.cleanup_node(node)

        node_type = obj.get(NODE_TYPE, None)
        # Cleanup values for Boolean, Int and Float types
        if node_type:
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
                    items = get_list_values(value)
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
                print('No "type" property in node')
                #raise Exception('No "type" property in node')

        return obj2

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
        with self.driver.session(database = self.db_name) as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.prepare_node(org_obj)
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
        with self.driver.session(database = self.db_name) as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    line_num += 1
                    obj = self.prepare_node(org_obj)
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
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')

            row = next(reader)
            row = self.cleanup_node(row)
            row_prepare_node = self.prepare_node(row)
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
            if len(parent_error_list) > 0:
                for parent_error_field_name in parent_error_list:
                    self.log.error('Parent pointer: "{}" not found in data model'.format(parent_error_field_name))
                self.log.error('Parent pointer not found in the data model, abort loading!')
                return False
        return True

    # Validate file
    def validate_file(self, file_name, max_violations):
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            violations = 0
            ids = {}
            if not self.validate_field_name(file_name):
                return False

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
                        else:
                            # Same ID exists in same file, but properties are also same, probably it's pointing same
                            # object to multiple parents
                            self.log.debug(
                                f'Duplicated data at line {line_num}: duplicate {id_field}: {node_id}, found in line: '
                                f'{", ".join(ids[node_id]["lines"])}')
                    else:
                        ids[node_id] = {'props': get_props_signature(props), 'lines': [str(line_num)]}

                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj)
                if not validate_result['result'] and not validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.error('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    validation_failed = True
                    violations += 1
                    if violations >= max_violations:
                        return False
                elif not validate_result['result'] and validate_result['warning']:
                    for msg in validate_result['messages']:
                        self.log.warning('Invalid data at line {}: "{}"!'.format(line_num, msg))
            return not validation_failed

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
        statement += ' WHERE NOT (n)<--(m)-->() RETURN m'
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
    def convert_df_to_dict(self,file_data):
        total_records = len(file_data)
        file_data.columns = [i if len(i.split('.')) == 1 else  i.split('.')[1] for i in file_data.columns]
        qry_str = ['n.' + i + ' = row.' +  i   for i in file_data.columns]
        file_data = file_data.to_dict('records')
        
        return file_data, qry_str, total_records
    
    def process_data_in_batches(self, tx, data, create_type, node_type):
        new_qry =  """CALL apoc.periodic.iterate( """ 
        new_qry += """\"UNWIND $data AS item return item\", """  # Iterate statement: Unwinds the list of data items
        
        if create_type == "CREATE":
            new_qry += """\"CREATE (n:MyNode) SET n = item  SET n.created = datetime() return n \", """    # Action statement: Creates a node for each item
        elif create_type == 'MATCH':  #add filtering criteria to node
            new_qry += """ \"Match(n:MyNode) where Id(n) = item.Node_ID and n.Primary_Key_Value = item.Primary_Key_Value """
            new_qry += """  SET n.updated = datetime()  return n \", """
        
        new_qry += """{batchSize: 1000, """   # Process 1000 items per batch
        new_qry += """parallel: true, """    # Run batches sequentially
        new_qry += """iterateList: true, """ # process all batches at once
        new_qry += """params: { data: $data } } )"""   # Pass the data list as a parameter
        
        new_qry = new_qry.replace("MyNode", node_type)

        ##remove user created variables, do not need to be loaded into db
        new_qry = new_qry.replace("n.Node_Exists = item.Node_Exists","")
        new_qry = new_qry.replace("n.Node_ID = item.Node_ID","")
        new_qry = new_qry.replace("n.Primary_Key_Value = item.Primary_Key_Value","")

        result = tx.run(new_qry, data=data)
        data_list = [i for i in result.data()]
        return  data_list[0]
    
    def process_relationships_batches(self, tx, data, curr_qry):
        new_qry =  """CALL apoc.periodic.iterate( """ 
        new_qry += """\"UNWIND $data AS batch return batch\", """  # Iterate statement: Unwinds the list of data items
        new_qry += """ \"old_qry \", """
        
        new_qry += """{batchSize: 1000, """   # Process 1000 items per batch
        new_qry += """parallel: true, """    # Run batches sequentially
        new_qry += """iterateList: true, """ # process all batches at once
        new_qry += """params: { data: $data } } )"""   # Pass the data list as a parameter
        
        new_qry = new_qry.replace("old_qry", curr_qry)
        
        result = tx.run(new_qry, data=data)
        #result = tx.run(curr_qry, data)
        
        data_list = [i for i in result.data()]
        return  data_list[0]
    
    def write_nodes_to_db(self, session, col_name, node_type, current_nodes, create_type):

        file_data, qry_str, total_records = self.convert_df_to_dict(current_nodes)
        qry_result = session.execute_write(self.process_data_in_batches, file_data, create_type, node_type)
        self.load_passed += qry_result["operations"]["committed"]
        self.load_failed += qry_result["operations"]['failed']
    
    def load_nodes(self, session, tx, file_name, loading_mode,  wipe_db, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        elif loading_mode == DELETE_MODE:
            action_word = 'Deleting'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info('{} nodes from file: {}'.format(action_word, file_name))

        file_data = pd.read_csv(file_name, sep='\t', header=0)
        file_data.fillna("missing data", inplace=True)
        
        ## load nodes in batches of 5,000
        nodes_done = 0
        all_obj = []
        all_new_nodes = 0
        all_existing_nodes = 0
        batches = 0
        
        # remove duplicate records by primay ID (This value should be unique)
        file_data = file_data.drop_duplicates(self.schema.get_id_field(file_data.iloc[0].to_dict()))

        while nodes_done < len(file_data):
            batch_size = 5000
            batch_time = time.time()
            if len(file_data) <= batch_size:
                start_node = 0
                end_node = len(file_data)
            elif (nodes_done+batch_size) > len(file_data):
                start_node = nodes_done
                end_node = len(file_data)
            else: 
                start_node = nodes_done
                end_node = nodes_done + batch_size
            data_to_work = file_data[start_node: end_node]

            data_to_work.reset_index(inplace = True, drop=True)
            for index in data_to_work.index:
                obj = self.prepare_node(data_to_work.iloc[index].to_dict())  #formats incomming data
                all_obj.append(obj)
    
            df = pd.DataFrame(all_obj[start_node: end_node])
            col_name = self.schema.get_id_field(obj)  #primary key field
            node_type = obj[NODE_TYPE]   #current node to match
    
            #current_nodes = self.node_keys_dict[node_type]
            #if len(current_nodes["Node_Index_DF"]) == 0:
            #    print(f"Node: {node_type} is empty")
                
            if loading_mode != DELETE_MODE:
                check_db = df.merge(self.node_keys_dict[node_type]['Node_Index_DF'], 
                                    left_on=self.schema.get_id_field(obj), right_on= "Primary_Key_Value", 
                                    how="left", indicator="Node_Exists")
                
                ## if the node does not already exist on primary key then create it
                new_nodes = check_db.query("Node_Exists == 'left_only'")   #use create
                if len(new_nodes) > 0:
                    self.write_nodes_to_db(session, col_name, node_type, new_nodes, "CREATE")
                 
                ## if the node does  already exist on primary key then update it using the merge statement
                existing_nodes = check_db.query("Node_Exists == 'both'")   #use merge
                if len(existing_nodes ) > 0:
                    self.write_nodes_to_db(session, col_name, node_type, existing_nodes, "MATCH")
            all_existing_nodes += len(existing_nodes)
            all_new_nodes += len(new_nodes)
            
            nodes_done += len(data_to_work)
            batches += 1
            end_time = time.time()
            print(f"Completed batch {batches}, total nodes done: {nodes_done} in {end_time - batch_time} seconds")
        return all_new_nodes, all_existing_nodes , all_obj


    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{0} {{ {1}: ''${1}'' }}) return m'.format(label, prop)
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
    def has_existing_relationship(self, session, node_type, node, relationship, curr_index,  count_same_parent=False):
        relationship_name = relationship[RELATIONSHIP_TYPE]
        parent_type = relationship[PARENT_TYPE]
        parent_id_field = relationship[PARENT_ID_FIELD]

        base_statement = 'MATCH (n:{0})-[r:{2}]->(m:{3}) where n.{1} = ${1} and ID(n) = {4} '.format(node_type,
                                                                                 self.schema.get_id_field(node),
                                                                                 relationship_name, parent_type, curr_index)
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

    def remove_old_relationship(self, session, node_type, node, relationship, curr_index):
        del_statement = self.has_existing_relationship(session, node_type, node, relationship, curr_index)
        if del_statement:
            del_result = session.run(del_statement, node)
            if not del_result:
                self.log.error('Delete old relationship failed!')

    def load_relationships(self, session, file_data, loading_mode, split=False):
        #if loading_mode == NEW_MODE:
        #    action_word = 'Loading new'
        #elif loading_mode == UPSERT_MODE:
        #    action_word = 'Loading'
        #else:
        #    raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        #self.log.info('{} relationships from file: {}'.format(action_word, file_name))
        
        
        #file_encoding = check_encoding(file_name)
        #with open(file_name, encoding=file_encoding) as in_file:
        #    reader = csv.DictReader(in_file, delimiter='\t')
        relationships_created = {}
        int_nodes_created = 0
        line_num = 1
        transaction_counter = 0
        
        file_data_df = pd.DataFrame(file_data)
        obj = file_data_df.iloc[0].to_dict()

        node_type = obj[NODE_TYPE]
        file_data_df = file_data_df.merge(self.node_keys_dict[node_type]['Node_Index_DF'], 
                                    left_on=self.schema.get_id_field(obj), right_on= "Primary_Key_Value", 
                                    how="left", indicator="Node_Exists")

        file_data_df.fillna(-1, inplace=True)
        missing_id = file_data_df.query("Node_ID == -1")
        if len(missing_id) > 0:
            print("x")
        
        ids = self.schema.props.id_fields
        header_names = [f"{curr_id}.{ids[curr_id]}" for curr_id in ids]
        
        check_relationship = [i for i in file_data_df.columns if i in header_names]
        if len(check_relationship) == 0:
            print(f"no relationship found for file type: {list(set(file_data_df[NODE_TYPE]))}")
        else:
            for curr_relationship in check_relationship:

                try:
                    
                    relation = self.schema.relationships[node_type][curr_relationship.split('.')[0]]['relationship_type']
                    qry_str = "  "
                    qry_str += f"MATCH (m:{curr_relationship.split('.')[0]}) "
                    qry_str += f"MATCH (n:{node_type}) "
                    qry_str += f"where m.{curr_relationship.split('.')[1]} = batch.{curr_relationship.split('.')[1]} and "
                    
                    qry_str += 'n.{0} = batch.{0} and ID(n) = batch.Node_ID '.format(self.schema.get_id_field(obj))
                    qry_str += f"MERGE (n)-[r:{relation}]->(m) ON CREATE SET r.created = datetime() ON MATCH SET r.updated = datetime()"
                    
                    
                    file_data_df.columns = [i if len(i.split('.')) == 1 else  i.split('.')[1] for i in file_data_df.columns]
                    file_data_dct = file_data_df.to_dict('records')
                    qry_result = session.execute_write(self.process_relationships_batches, file_data_dct, qry_str)
                    
                    self.relationship_passed += qry_result["operations"]['committed']
                    self.relationship_failed += qry_result["operations"]['failed']
                except Exception as e:
                    print(file_data_dct)
        return

        # Use session in one transaction mode
        tx = session
        # Use transactions in split-transactions mode
        if split:
            tx = session.begin_transaction()
        for obj in file_data:
            line_num += 1
            
            transaction_counter += 1
            #obj = self.prepare_node(org_obj)
            
            print(transaction_counter)
            
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
                    
                    curr_index = self.node_keys_dict[node_type]["Node_Index_DF"].query(f"Primary_Key_Value == '{obj[self.schema.get_id_field(obj)]}'")["Node_ID"].iloc[0]
                    if multiplier in [DEFAULT_MULTIPLIER, ONE_TO_ONE]:
                        if loading_mode == UPSERT_MODE:
                            self.remove_old_relationship(tx, node_type, obj, relationship, curr_index)
                        elif loading_mode == NEW_MODE:
                            if self.has_existing_relationship(tx, node_type, obj, relationship, curr_index,  True):
                                raise Exception(
                                    'Line: {}: Relationship already exists, abort loading!'.format(line_num))
                        else:
                            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
                    else:
                        self.log.debug('Multiplier: {}, no action needed!'.format(multiplier))
                    prop_statement = ', '.join(self.get_relationship_prop_statements(properties))
                                           
                    statement = 'MATCH (m:{0} {{ {1}: ${1} }})'.format(parent_node, parent_id_field)
                    statement += ' MATCH (n:{0}) where n.{1} = ${1} and ID(n) = {2}'.format(node_type,
                                                                         self.schema.get_id_field(obj), curr_index)
                    statement += ' MERGE (n)-[r:{}]->(m)'.format(relationship_name)
                    statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                    statement += ', {}'.format(prop_statement) if prop_statement else ''
                    statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)
                    statement += ', {}'.format(prop_statement) if prop_statement else ''

                    result = tx.run(statement, {**obj, parent_id_field: parent_id, **properties})
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
    
    
    
    
    def clean_database(self, tx):
        batch_size = 10000
        self.log.info(" ")
        query = """ 
            MATCH ()-[r]->()
            CALL {
                WITH r
                DELETE r
                } IN TRANSACTIONS OF $batch_size ROWS
            """
        tx.run(query, batch_size=batch_size)
        self.log.info("all relationships deleted!")
        self.log.info(" ")
        self.log.info(" ")

        query = """
             CALL apoc.periodic.iterate(
            "MATCH (n) RETURN n",
            "DETACH DELETE n",
            {batchSize: $batch_size, parallel: true} ) """
        tx.run(query, batch_size=batch_size)
        self.log.info("all nodes deleted!")
        self.log.info(" ")
        
        
        #query = """  MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS"""
        #result = tx.run(query).consume()
        
        #return node_result

    def wipe_db(self, session, split=False):
        session.execute_write(self.clean_database)
        #self.nodes_deleted = result.counters.nodes_deleted
        #self.relationships_deleted = result.counters.relationships_deleted
        #self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
        #self.log.info('{} relationships deleted!'.format(self.relationships_deleted))

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

    def create_indexes(self, session):
        """
        Creates indexes, if they do not already exist, for all entries in the "id_fields" and "indexes" sections of the
        properties file
        :param session: the current neo4j transaction session
        """
        existing = get_btree_indexes(session)
        # Create indexes from "id_fields" section of the properties file
        ids = self.schema.props.id_fields
        for node_name in ids:
            self.create_index(node_name, ids[node_name], existing, session)
        # Create indexes from "indexes" section of the properties file
        indexes = self.schema.props.indexes
        # each index is a dictionary, indexes is a list of these dictionaries
        # for each dictionary in list
        for node_dict in indexes:
            node_name = list(node_dict.keys())[0]
            self.create_index(node_name, node_dict[node_name], existing, session)

    def create_index(self, node_name, node_property, existing, session):
        index_tuple = format_as_tuple(node_name, node_property)
        # If node_property is a list of properties, convert to a comma delimited string
        if isinstance(node_property, list):
            node_property = ",".join(node_property)
        if index_tuple not in existing:
            command = "CREATE INDEX ON :{}({});".format(node_name, node_property)
            session.run(command)
            self.indexes_created += 1
            self.log.info("Index created for \"{}\" on property \"{}\"".format(node_name, node_property))