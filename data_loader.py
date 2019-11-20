#!/usr/bin/env python3

import os
import csv
import re
from neo4j import  Driver, Session, Transaction
from utils import *
from timeit import default_timer as timer
from icdc_schema import ICDC_Schema
from datetime import datetime, timedelta

NODE_TYPE = 'type'
VISIT_NODE = 'visit'
VISIT_ID = 'visit_id'
VISIT_DATE = 'visit_date'
PROP_TYPE = 'Type'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
START_DATE = 'date_of_cycle_start'
END_DATE = 'date_of_cycle_end'
OF_CYCLE = 'of_cycle'
CYCLE_NODE = 'cycle'
excluded_fields = {NODE_TYPE}
CASE_NODE = 'case'
CASE_ID = 'case_id'
PREDATE = 7
FOREVER = '9999-12-31'
INFERRED = 'inferred'
CREATED = 'created'
UPDATED = 'updated'
RELATIONSHIPS = 'relationships'
VISITS_CREATED = 'visits_created'
PROVIDED_PARENTS = 'provided_parents'


class DataLoader:
    def __init__(self, driver, schema):
        if not driver or not isinstance(driver, Driver):
            raise Exception('Invalid Neo4j driver object')
        elif not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.log = get_logger('Data Loader')
        self.driver = driver
        self.schema = schema

    def check_files(self, file_list):
        if not file_list:
            self.log.error('Invalid file list')
            return False
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    self.log.error('File "{}" doesn\'t exist'.format(data_file))
                    return False
            return True

    def load(self, file_list, cheat_mode, dry_run, max_violations):
        if not self.check_files(file_list):
            return False
        start = timer()
        if not cheat_mode:
            validation_failed = False
            for txt in file_list:
                if not self.validate_file(txt, max_violations) :
                    self.log.error('Validating file "{}" failed!'.format(txt))
                    validation_failed = True
            if validation_failed:
                return False
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')

        if dry_run:
            end = timer()
            self.log.info('Dry run mode, no nodes or relationships loaded.')  # Time in seconds, e.g. 5.38091952400282
            self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
            return {NODES_CREATED: 0, RELATIONSHIP_CREATED: 0}

        self.nodes_created = 0
        self.relationships_created = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        with self.driver.session() as session:
            tx = session.begin_transaction()
            try:
                for txt in file_list:
                    self.load_nodes(tx, txt)
                for txt in file_list:
                    self.load_relationships(tx, txt)
                tx.commit()
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                return False
        end = timer()

        # Print statistics
        for node in sorted(self.nodes_stat.keys()):
            count = self.nodes_stat[node]
            self.log.info('Node: (:{}) loaded: {}'.format(node, count))
        for rel in sorted(self.relationships_stat.keys()):
            count = self.relationships_stat[rel]
            self.log.info('Relationship: [:{}] loaded: {}'.format(rel, count))
        self.log.info('{} nodes and {} relationships loaded!'.format(self.nodes_created, self.relationships_created))
        self.log.info('Loading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created}


    # Remove extra spaces at begining and end of the keys and values
    # Add uuid to nodes if one not exists
    def cleanup_node(self, node):
        obj = {}
        for key, value in node.items():
            obj[key.strip()] = value.strip()

        if UUID not in obj:
            id_field = self.schema.get_id_field(obj)
            id = self.schema.get_id(obj)
            node_type = obj.get(NODE_TYPE)
            if node_type:
                if not id:
                    obj[UUID] = get_uuid_for_node(node_type, self.get_signature(obj))
                elif id_field != UUID:
                    obj[UUID] = get_uuid_for_node(node_type, id)
            else:
                raise Exception('No "type" property in node')
        return obj

    def get_signature(self, node):
        result = []
        for key, value in node.items():
            result.append('{}: {}'.format(key, value))
        return '{{ {} }}'.format(', '.join(result))

    # Validate all cases exist in a data (TSV/TXT) file
    def validate_cases_exist_in_file(self, file_name, max_violations):
        with self.driver.session() as session:
            with open(file_name) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.cleanup_node(org_obj)
                    line_num += 1
                    # Validate parent exist
                    if CASE_ID in obj:
                        case_id = obj[CASE_ID]
                        if not self.node_exists(session, CASE_NODE, CASE_ID, case_id):
                            self.log.error(
                                'Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) doesn\'t exist!'.format(
                                    line_num, CASE_NODE, CASE_ID, case_id))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                return not validation_failed

    # Validate all parents exist in a data (TSV/TXT) file
    def validate_parents_exist_in_file(self, file_name, max_violations):
        validation_failed = True
        with self.driver.session() as session:
            with open(file_name) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    line_num += 1
                    obj = self.cleanup_node(org_obj)
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

    # Validate file
    def validate_file(self, file_name, max_violations):
        with open(file_name) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            violations = 0
            IDs = {}
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                line_num += 1
                id_field = self.schema.get_id_field(obj)
                node_id = self.schema.get_id(obj)
                if node_id:
                    if node_id in IDs:
                        validation_failed = True
                        self.log.error('Invalid data at line {}: duplicate {}: {}, found in line: {}'.format(line_num,
                                                                                                             id_field, node_id, ', '.join(IDs[node_id])))
                        IDs[node_id].append(str(line_num))
                    else:
                        IDs[node_id] = [str(line_num)]

                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj)
                if not validate_result['result']:
                    for msg in validate_result['messages']:
                        self.log.error('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    validation_failed = True
                    violations += 1
                    if violations >= max_violations:
                        return False
            return not validation_failed

    # load file
    def load_nodes(self, session, file_name):
        self.log.info('Loading nodes from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            line_num = 1
            for org_obj in reader:
                line_num += 1
                obj = self.cleanup_node(org_obj)
                node_type = obj[NODE_TYPE]
                node_id = self.schema.get_id(obj)
                id_field = self.schema.get_id_field(obj)
                # statement is used to create current node
                statement = ''
                # prop_statement set properties of current node
                if node_id:
                    prop_statement = 'SET n.{} = {{node_id}}'.format(id_field)
                else:
                    raise Exception('Line:{}: No ids found!'.format(line_num))
                    prop_statement = []

                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif key == id_field:
                        continue

                    field_name = key
                    if is_parent_pointer(key):
                        header = key.split('.')
                        if len(header) > 2:
                            self.log.warning('Column header "{}" has multiple periods!'.format(key))
                        field_name = header[1]
                        parent = header[0]
                        combined = '{}_{}'.format(parent, field_name)
                        if field_name in obj:
                            self.log.warning('"{}" field is in both "{}" and parent "{}", use "{}" instead !'.format(
                                key, node_type, parent, combined))
                            field_name = combined

                    value_string = self.get_value_string(node_type, field_name, value)
                    if node_id:
                        if value_string is not None:
                            prop_statement += ', n.{} = {}'.format(field_name, value_string)
                        for extra_prop_name, extra_value in self.schema.get_extra_props(node_type, key, value).items():
                            extra_value_string = self.get_value_string(node_type, extra_prop_name, extra_value)
                            if extra_value_string is not None:
                                prop_statement += ', n.{} = {}'.format(extra_prop_name, extra_value_string)
                    else:
                        if value_string is not None:
                            prop_statement.append('{}: {}'.format(field_name, value_string))
                        for extra_prop_name, extra_value in self.schema.get_extra_props(node_type, key, value).items():
                            extra_value_string = self.get_value_string(node_type, extra_prop_name, extra_value)
                            if extra_value_string is not None:
                                prop_statement.append('{}: {}'.format(extra_prop_name, extra_value_string))

                if node_id:
                    statement += 'MERGE (n:{} {{ {}: {{node_id}} }})'.format(node_type, id_field)
                    statement += ' ON CREATE ' + prop_statement + ' ,n.{} = datetime()'.format(CREATED)
                    statement += ' ON MATCH ' + prop_statement + ' ,n.{} = datetime()'.format(UPDATED)
                else:
                    statement += 'MERGE (n:{} {{ {} }})'.format(node_type, ', '.join(prop_statement))
                    statement += ' ON CREATE SET n.{} = datetime()'.format(CREATED)
                    statement += ' ON MATCH SET n.{} = datetime()'.format(UPDATED)

                result = session.run(statement, {"node_id": node_id})
                count = result.summary().counters.nodes_created
                self.nodes_created += count
                nodes_created += count
                self.nodes_stat[node_type] = self.nodes_stat.get(node_type, 0) + count
            self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, node_type))

    def get_value_string2(self, key):
        if not key or not isinstance(key, str):
            return None
        return '{{{}}}'.format(key)

    def get_value_string(self, node_type, key, value):
        key_type = self.schema.get_prop_type(node_type, key)
        if key_type == 'String' or key_type == 'Date' or key_type == 'DateTime':
            if isinstance(value, str):
                value_string = '"{}"'.format(value)
            else:
                value_string = None
        elif key_type == 'Boolean':
            cleaned_value = None
            if isinstance(value, str):
                if re.search(r'yes|true', value, re.IGNORECASE):
                    cleaned_value = True
                elif re.search(r'no|false', value, re.IGNORECASE):
                    cleaned_value = False
                else:
                    self.log.debug('Unsupported Boolean value: "{}"'.format(value))
                    cleaned_value = None
            if cleaned_value is not None:
                value_string = '{}'.format(cleaned_value)
            else:
                value_string = '""'
        elif key_type == 'Int':
            try:
                if value is None:
                    value_string = None
                else:
                    value_string = int(value)
            except Exception:
                value_string = None
        elif key_type == 'Float':
            try:
                if value is None:
                    value_string = None
                else:
                    value_string = float(value)
            except Exception:
                value_string = None
        # Other types
        else:
            self.log.warning('Value type: "{}" is not supported!'.format(key_type))
            value_string = value
        return value_string

    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{0} {{ {1}: {{{1}}} }}) return m'.format(label, prop)
        result = session.run(statement, {prop: value})
        count = result.detach()
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def collect_relationships(self, obj, session, create_visit, line_num) -> dict:
        node_type = obj[NODE_TYPE]
        relationships = []
        visits_created = 0
        provided_parents = 0
        for key, value in obj.items():
            if is_parent_pointer(key):
                provided_parents += 1
                other_node, other_id = key.split('.')
                relationship = self.schema.get_relationship(node_type, other_node)
                relationship_name = relationship[RELATIONSHIP_TYPE]
                multiplier = relationship[MULTIPLIER]
                if not relationship_name:
                    self.log.error('Line: {}: Relationship not found!'.format(line_num))
                    raise Exception('Undefined relationship, abort loading!')
                if not self.node_exists(session, other_node, other_id, value):
                    if other_node == 'visit' and create_visit:
                        if self.create_visit(session, line_num, other_node, value, obj):
                            visits_created += 1
                            relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                                  RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
                        else:
                            self.log.error(
                                'Line: {}: Couldn\'t create {} node automatically!'.format(line_num, VISIT_NODE))
                    else:
                        self.log.warning(
                            'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node, other_id,
                                                                                                   value))
                else:
                    if multiplier == ONE_TO_ONE and self.parent_already_has_child(session, node_type, self.get_search_criteria_for_node(obj),
                                                                                  relationship_name, other_node, other_id, value):
                        self.log.warning('Line: {}: one_to_one relationship failed, parent already has a child!'.format(line_num))
                    else:
                        relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                          RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
        return {RELATIONSHIPS: relationships, VISITS_CREATED: visits_created, PROVIDED_PARENTS: provided_parents}

    def parent_already_has_child(self, session, node_type, criteria_statement, relationship_name, parent_type, parent_id_field, parent_id):

        statement = 'MATCH (n:{})-[r:{}]->(m:{} {{ {}: {{parent_id}} }}) return n'.format(node_type, relationship_name, parent_type, parent_id_field)
        result = session.run(statement, {"parent_id": parent_id})
        if result:
            child = result.single()
            if child:
                find_current_node_statement = 'MATCH (n:{} {{ {} }}) return n'.format(node_type, criteria_statement)
                current_node_result = session.run(find_current_node_statement)
                if current_node_result:
                    current_node = current_node_result.single()
                    return child[0].id != current_node[0].id
                else:
                    self.log.error('Could NOT find current node!')

        return False


    def remove_old_relationship(self, session, node_type, criteria_statement, relationship):
        relationship_name = relationship[RELATIONSHIP_TYPE]
        parent_type = relationship[PARENT_TYPE]
        parent_id_field = relationship[PARENT_ID_FIELD]

        base_statement = 'MATCH (n:{} {{ {} }})-[r:{}]->(m:{})'.format(node_type, criteria_statement, relationship_name, parent_type)
        statement = base_statement + ' return m.{} AS {}'.format(parent_id_field, PARENT_ID)
        result = session.run(statement)
        if result:
            old_parent = result.single()
            if old_parent:
                old_parent_id = old_parent[PARENT_ID]
                if old_parent_id != relationship[PARENT_ID]:
                    self.log.warning('Old parent is different from new parent, delete relationship to old parent: (:{} {{ {}: "{}" }})!'.format(parent_type,
                                                                                                                                                parent_id_field, old_parent_id))
                    del_statement = base_statement + ' delete r'
                    del_result = session.run(del_statement)
                    if not del_result:
                        self.log.error('Delete old relationship failed!')
        else:
            self.log.error('Remove old relationship failed: Query old relationship failed!')



    def load_relationships(self, session, file_name):
        self.log.info('Loading relationships from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            relationships_created = {}
            visits_created = 0
            line_num = 1
            for org_obj in reader:
                line_num += 1
                obj = self.cleanup_node(org_obj)
                node_type = obj[NODE_TYPE]
                # criteria_statement is used to find current node
                criteria_statement = self.get_search_criteria_for_node(obj)
                results = self.collect_relationships(obj, session, True, line_num)
                relationships = results[RELATIONSHIPS]
                visits_created += results[VISITS_CREATED]
                provided_parents = results[PROVIDED_PARENTS]
                if provided_parents > 0:
                    if len(relationships) == 0:
                        raise Exception('Line: {}: No parents found, abort loading!'.format(line_num))

                    for relationship in relationships:
                        relationship_name = relationship[RELATIONSHIP_TYPE]
                        multiplier = relationship[MULTIPLIER]
                        parent_node = relationship[PARENT_TYPE]
                        parent_id_field = relationship[PARENT_ID_FIELD]
                        parent_id = relationship[PARENT_ID]
                        if multiplier in [DEFAULT_MULTIPLIER, ONE_TO_ONE]:
                            self.remove_old_relationship(session, node_type, criteria_statement, relationship)
                        else:
                            self.log.info('Multiplier: {}, no action needed!'.format(multiplier))
                        statement = 'MATCH (m:{0} {{ {1}: {{{1}}} }}) '.format(parent_node, parent_id_field)
                        statement += 'MATCH (n:{} {{ {} }}) '.format(node_type, criteria_statement)
                        statement += 'MERGE (n)-[r:{}]->(m)'.format(relationship_name)
                        statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                        statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)

                        result = session.run(statement, {parent_id_field: parent_id})
                        count = result.summary().counters.relationships_created
                        self.relationships_created += count
                        relationship_pattern = '(:{})->[:{}]->(:{})'.format(node_type, relationship_name, parent_node)
                        relationships_created[relationship_pattern] = relationships_created.get(relationship_pattern, 0) + count
                        self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + count

            for rel, count in relationships_created.items():
                self.log.info('{} {} relationship(s) loaded'.format(count, rel))
            if visits_created > 0:
                self.log.info('{} (:{}) node(s) loaded'.format(visits_created, VISIT_NODE))

        return True

    def get_search_criteria_for_node(self, node):
        id_field = self.schema.get_id_field(node)
        node_id = self.schema.get_id(node)
        node_type = node[NODE_TYPE]
        if node_id:
            criteria_statement = '{}: "{}"'.format(id_field, node_id)
        else:
            self.log.warning('{} field is missing or empty, try to use {} as ID'.format(id_field, UUID))
            if UUID in node:
                criteria_statement = '{}: "{}"'.format(UUID, node[UUID])
            else:
                raise Exception('Node does NOT have any IDs')
        return criteria_statement

    def create_visit(self, session, line_num, node_type, node_id, src):
        if node_type != VISIT_NODE:
            self.log.error("Line: {}: Can't create (:{}) node for type: '{}'".format(line_num, VISIT_NODE, node_type))
            return False
        if not node_id:
            self.log.error("Line: {}: Can't create (:{}) node for id: '{}'".format(line_num, VISIT_NODE, node_id))
            return False
        if not src:
            self.log.error("Line: {}: Can't create (:{}) node for empty object".format(line_num, VISIT_NODE))
            return False
        if not session or (not isinstance(session, Session) and not isinstance(session, Transaction)):
            self.log.error("Neo4j session is not valid!")
            return False
        date_map = PROPS['visit_date_in_nodes']
        if NODE_TYPE not in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        source_type = src[NODE_TYPE]
        date = src[date_map[source_type]]
        if not date:
            self.log.error('Line: {}: Visit date is empty!'.format(line_num))
            return False
        if not NODE_TYPE in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        statement = 'MERGE (v:{} {{ {}: {{node_id}}, {}: {{date}}, {}: true }})'.format(VISIT_NODE, VISIT_ID, VISIT_DATE, INFERRED)
        statement += ' ON CREATE SET v.{} = datetime()'.format(CREATED)
        statement += ' ON MATCH SET v.{} = datetime()'.format(UPDATED)

        result = session.run(statement, {"node_id": node_id, "date": date})
        if result:
            count = result.summary().counters.nodes_created
            self.nodes_created += count
            self.nodes_stat[VISIT_NODE] = self.nodes_stat.get(VISIT_NODE, 0) + count
            if count > 0:
                case_id = src[CASE_ID]
                if not self.connect_visit_to_cycle(session, line_num, node_id, case_id, date):
                    self.log.error('Line: {}: Visit: "{}" does NOT belong to a cycle!'.format(line_num, node_id))
                return True
        else:
            return False

    def connect_visit_to_cycle(self, session, line_num, visit_id, case_id, visit_date):
        find_cycles_stmt = 'MATCH (c:cycle) WHERE c.case_id = {case_id} RETURN c ORDER BY c.date_of_cycle_start'
        result = session.run(find_cycles_stmt, {'case_id': case_id})
        if result:
            first_date = None
            pre_date = None
            relationship_name = self.schema.get_relationship(VISIT_NODE, CYCLE_NODE)[RELATIONSHIP_TYPE]
            if not relationship_name:
                return False
            for record in result.records():
                cycle = record.data()['c']
                date = datetime.strptime(visit_date, DATE_FORMAT)
                start_date = datetime.strptime(cycle[START_DATE], DATE_FORMAT)
                if not first_date:
                    first_date = start_date
                    pre_date = first_date - timedelta(days=PREDATE)
                if cycle[END_DATE]:
                    end_date = datetime.strptime(cycle[END_DATE], DATE_FORMAT)
                else:
                    self.log.warning('Line: {}: No end dates for cycle started on {} for {}'.format(line_num, start_date.strftime(DATE_FORMAT), case_id))
                    end_date = datetime.strptime(FOREVER, DATE_FORMAT)
                if (date >= start_date and date <= end_date) or (date < first_date and date >= pre_date):
                    if date < first_date and date >= pre_date:
                        self.log.info('Line: {}: Date: {} is before first cycle, but within {} days before first cycle started: {}, connected to first cycle'.format(line_num,
                                                                                                                                                                     visit_date, PREDATE, first_date.strftime(DATE_FORMAT)))
                    cycle_id = cycle.id
                    connect_stmt = 'MATCH (v:{} {{ {}: {{visit_id}} }}) MATCH (c:{}) WHERE id(c) = {{cycle_id}} MERGE (v)-[r:{} {{ {}: true }}]->(c)'.format(VISIT_NODE, VISIT_ID, CYCLE_NODE, relationship_name, INFERRED)
                    connect_stmt += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                    connect_stmt += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)

                    cnt_result = session.run(connect_stmt, {'visit_id': visit_id, 'cycle_id': cycle_id})
                    relationship_created = cnt_result.summary().counters.relationships_created
                    if relationship_created > 0:
                        self.relationships_created += relationship_created
                        self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + relationship_created
                        return True
                    else:
                        self.log.error('Line: {}: Create (:visit)-[:of_cycle]->(:cycle) relationship failed!'.format(line_num))
                        return False
            self.log.warning('Line: {}: Date: {} does not belong to any cycles, connected to case {} directly!'.format(
                    line_num, visit_date, case_id))
            return self.connect_visit_to_case(session, line_num, visit_id, case_id)
        else:
            self.log.error('Line: {}: No cycles found for case: {}'.format(line_num, case_id))
            return False

    def connect_visit_to_case(self, session, line_num, visit_id, case_id):
        relationship_name = self.schema.get_relationship(VISIT_NODE, CASE_NODE)[RELATIONSHIP_TYPE]
        if not relationship_name:
            return False
        cnt_statement = 'MATCH (c:case {{ case_id: {{case_id}} }}) MATCH (v:visit {{ {}: {{visit_id}} }}) '.format(VISIT_ID)
        cnt_statement += 'MERGE (c)<-[r:{} {{ {}: true }}]-(v)'.format(relationship_name, INFERRED)
        cnt_statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
        cnt_statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)

        result = session.run(cnt_statement, {'case_id': case_id, 'visit_id': visit_id})
        relationship_created = result.summary().counters.relationships_created
        if relationship_created > 0:
            self.relationships_created += relationship_created
            self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + relationship_created
            return True
        else:
            self.log.error('Line: {}: Create (:{})-[:{}]->(:{}) relationship failed!'.format(line_num, VISIT_NODE, relationship_name, CASE_NODE))
            return False


