import os
import re
import sys

import yaml

from bento.common.utils import get_logger, MULTIPLIER, DEFAULT_MULTIPLIER, RELATIONSHIP_TYPE, get_uuid, \
    parse_date
from props import Props

NODES = 'Nodes'
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'String'
PROP_TYPE = 'Type'
PROP_ENUM = 'Enum'
END_POINTS = 'Ends'
SRC = 'Src'
DEST = 'Dst'
VALUE_TYPE = 'value_type'
ITEM_TYPE = 'item_type'
#LIST_DELIMITER = '|'
LABEL_NEXT = 'next'
NEXT_RELATIONSHIP = 'next'
UNITS = 'units'
REQUIRED = 'Req'
PRIVATE = 'Private'
NODE_TYPE = 'type'
ENUM = 'enum'
DEFAULT_VALUE = 'default_value'
HAS_UNIT = 'has_unit'
MIN = 'minimum'
MAX = 'maximum'
EX_MIN = 'exclusiveMinimum'
EX_MAX = 'exclusiveMaximum'
DESCRIPTION = 'Desc'


def is_parent_pointer(field_name):
    return re.fullmatch(r'\w+\.\w+', field_name) is not None


class ICDC_Schema:
    def __init__(self, yaml_files, props):
        if not isinstance(props, Props):
            raise AssertionError
        self.props = props
        self.rel_prop_delimiter = props.rel_prop_delimiter
        self.delimiter = props.delimiter

        if not yaml_files:
            raise Exception('File list is empty,could not initialize ICDC_Schema object!')
        else:
            for data_file in yaml_files:
                if not os.path.isfile(data_file):
                    raise Exception('File "{}" does not exist'.format(data_file))
        self.log = get_logger('ICDC Schema')
        self.org_schema = {}
        for aFile in yaml_files:
            try:
                self.log.info('Reading schema file: {} ...'.format(aFile))
                if os.path.isfile(aFile):
                    with open(aFile) as schema_file:
                        schema = yaml.safe_load(schema_file)
                        if schema:
                            self.org_schema.update(schema)
            except Exception as e:
                self.log.exception(e)

        self.nodes = {}
        self.relationships = {}
        self.relationship_props = {}
        self.num_relationship = 0

        self.log.debug("-------------processing nodes-----------------")
        if NODES not in self.org_schema:
            self.log.error('Can\'t load any nodes!')
            sys.exit(1)

        elif PROP_DEFINITIONS not in self.org_schema:
            self.log.error('Can\'t load any properties!')
            sys.exit(1)

        for key, value in self.org_schema[NODES].items():
            # Assume all keys start with '_' are not regular nodes
            if not key.startswith('_'):
                self.process_node(key, value)
        self.log.debug("-------------processing edges-----------------")
        if RELATIONSHIPS in self.org_schema:
            for key, value in self.org_schema[RELATIONSHIPS].items():
                # Assume all keys start with '_' are not regular nodes
                if not key.startswith('_'):
                    self.process_node(key, value, True)
                    self.num_relationship += self.process_edges(key, value)

    def get_uuid_for_node(self, node_type, signature):
        """
        Generate V5 UUID for a node Arguments: node_type - a string represents type of a node, e.g. case, study,
        file etc. signature - a string that can uniquely identify a node within it's type, e.g. case_id,
        clinical_study_designation etc. or a long string with all properties and values concat together if no id
        available

        """
        return get_uuid(self.props.domain, node_type, signature)

    def _process_properties(self, desc):
        """
        Gather properties from description

        :param desc: description of properties
        :return: a dict with properties, required property list and private property list
        """
        props = {}
        required = set()
        private = set()
        if PROPERTIES in desc and desc[PROPERTIES] is not None:
            for prop in desc[PROPERTIES]:
                prop_type = self.get_type(prop)
                props[prop] = prop_type
                value_unit_props = self.process_value_unit_type(prop, prop_type)
                if value_unit_props:
                    props.update(value_unit_props)
                if self.is_required_prop(prop):
                    required.add(prop)
                if self.is_private_prop(prop):
                    private.add(prop)

        return {PROPERTIES: props, REQUIRED: required, PRIVATE: private}

    def get_list_values(self, list_str):
        return [item.strip() for item in list_str.split(self.delimiter) if item.strip()]
    def process_node(self, name, desc, is_relationship=False):
        """
        Process input node/relationship properties and save it in self.nodes

        :param name: node/relationship name
        :param desc:
        :param is_relationship: if input is a relationship
        :return:
        """
        properties = self._process_properties(desc)

        # All nodes and relationships that has properties will be save to self.nodes
        # Relationship without properties will be ignored
        if properties[PROPERTIES] or not is_relationship:
            self.nodes[name] = properties

    def process_edges(self, name, desc):
        count = 0
        if MULTIPLIER in desc:
            multiplier = desc[MULTIPLIER]
        else:
            multiplier = DEFAULT_MULTIPLIER

        properties = self._process_properties(desc)
        self.relationship_props[name] = properties

        if END_POINTS in desc:
            for end_points in desc[END_POINTS]:
                src = end_points[SRC]
                dest = end_points[DEST]
                if MULTIPLIER in end_points:
                    actual_multiplier = end_points[MULTIPLIER]
                    self.log.debug(
                        'End point multiplier: "{}" overriding relationship multiplier: "{}"'.format(actual_multiplier,
                                                                                                     multiplier))
                else:
                    actual_multiplier = multiplier
                if src not in self.relationships:
                    self.relationships[src] = {}
                self.relationships[src][dest] = {RELATIONSHIP_TYPE: name, MULTIPLIER: actual_multiplier}

                count += 1
                if src in self.nodes:
                    self.add_relationship_to_node(src, actual_multiplier, name, dest)
                    # nodes[src][self.plural(dest)] = '[{}] @relation(name:"{}")'.format(dest, name)
                else:
                    self.log.error('Source node "{}" not found!'.format(src))
                if dest in self.nodes:
                    self.add_relationship_to_node(dest, actual_multiplier, name, src, True)
                    # nodes[dest][self.plural(src)] = '[{}] @relation(name:"{}", direction:IN)'.format(src, name)
                else:
                    self.log.error('Destination node "{}" not found!'.format(dest))
        return count

    # Process singular/plural array/single value based on relationship multipliers like  many-to-many, many-to-one etc.
    # Return a relationship property to add into a node
    def add_relationship_to_node(self, name, multiplier, relationship, other_node, dest=False):
        node = self.nodes[name]
        if multiplier == 'many_to_one':
            if dest:
                node[PROPERTIES][self.plural(other_node)] = {
                    PROP_TYPE: '[{}] @relation(name:"{}", direction:IN)'.format(other_node, relationship)}
            else:
                node[PROPERTIES][other_node] = {
                    PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(other_node, relationship)}
        elif multiplier == 'one_to_one':
            if relationship == NEXT_RELATIONSHIP:
                if dest:
                    node[PROPERTIES]['prior_' + other_node] = {
                        PROP_TYPE: '{} @relation(name:"{}", direction:IN)'.format(other_node, relationship)}
                else:
                    node[PROPERTIES]['next_' + other_node] = {
                        PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(other_node, relationship)}
            else:
                if dest:
                    node[PROPERTIES][other_node] = {
                        PROP_TYPE: '{} @relation(name:"{}", direction:IN)'.format(other_node, relationship)}
                else:
                    node[PROPERTIES][other_node] = {
                        PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(other_node, relationship)}
        elif multiplier == 'many_to_many':
            if dest:
                node[PROPERTIES][self.plural(other_node)] = {
                    PROP_TYPE: '[{}] @relation(name:"{}", direction:IN)'.format(other_node, relationship)}
            else:
                node[PROPERTIES][self.plural(other_node)] = {
                    PROP_TYPE: '[{}] @relation(name:"{}", direction:OUT)'.format(other_node, relationship)}
        else:
            self.log.warning('Unsupported relationship multiplier: "{}"'.format(multiplier))

    def is_required_prop(self, name):
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result = prop.get(REQUIRED, False)
            result = str(result).lower()
            if result == "true" or result == "yes":
                return True
        return False

    def is_private_prop(self, name):
        result = False
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result = prop.get(PRIVATE, False)
        return result

    def get_prop_type(self, node_type, prop):
        if node_type in self.nodes:
            node = self.nodes[node_type]
            if prop in node[PROPERTIES]:
                return node[PROPERTIES][prop][PROP_TYPE]
        return DEFAULT_TYPE

    def get_type(self, name):
        result = {PROP_TYPE: DEFAULT_TYPE}
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result[DESCRIPTION] = prop.get(DESCRIPTION, '')
            result[REQUIRED] = prop.get(REQUIRED, False)
            key = None
            if PROP_TYPE in prop:
                key = PROP_TYPE
            elif PROP_ENUM in prop:
                key = PROP_ENUM
            if key:
                prop_desc = prop[key]
                if isinstance(prop_desc, str):
                    result[PROP_TYPE] = self.map_type(prop_desc)
                elif isinstance(prop_desc, dict):
                    if VALUE_TYPE in prop_desc:
                        result[PROP_TYPE] = self.map_type(prop_desc[VALUE_TYPE])
                        if ITEM_TYPE in prop_desc:
                            item_type = self._get_item_type(prop_desc[ITEM_TYPE])
                            result[ITEM_TYPE] = item_type
                        if PROP_ENUM in prop_desc:
                            item_type = self._get_item_type(prop_desc[PROP_ENUM])
                            result[ITEM_TYPE] = item_type
                        if UNITS in prop_desc:
                            result[HAS_UNIT] = True
                elif isinstance(prop_desc, list):
                    enum = set()
                    r_url = re.compile(r"://")
                    url_list = list(filter(r_url.search, prop_desc))
                    if not(len(prop_desc) == 1 and len(url_list) == 1):
                        for t in prop_desc:
                            enum.add(t)
                    if len(enum) > 0:
                        result[ENUM] = enum
                else:
                    self.log.debug(
                        'Property type: "{}" not supported, use default type: "{}"'.format(prop_desc, DEFAULT_TYPE))

                # Add value boundary support
                if MIN in prop:
                    result[MIN] = float(prop[MIN])
                if MAX in prop:
                    result[MAX] = float(prop[MAX])
                if EX_MIN in prop:
                    result[EX_MIN] = float(prop[EX_MIN])
                if EX_MAX in prop:
                    result[EX_MAX] = float(prop[EX_MAX])

        return result

    def _get_item_type(self, item_type):
        if isinstance(item_type, str):
            return {PROP_TYPE: self.map_type(item_type)}
        elif isinstance(item_type, list):
            enum = set()
            r_url = re.compile(r"://")
            url_list = list(filter(r_url.search, item_type))
            if not(len(item_type) == 1 and len(url_list) == 1):
                for t in item_type:
                    enum.add(t)
            if len(enum) > 0:
                return {PROP_TYPE: DEFAULT_TYPE, ENUM: enum}
            else:
                return None
        else:
            self.log.error(f"{item_type} is not a scala or Enum!")
            return None

    def get_prop(self, node_name, name):
        if node_name in self.nodes:
            node = self.nodes[node_name]
            if name in node[PROPERTIES]:
                return node[PROPERTIES][name]
        return None

    def get_default_value(self, node_name, name):
        prop = self.get_prop(node_name, name)
        if prop:
            return prop.get(DEFAULT_VALUE, None)

    def get_default_unit(self, node_name, name):
        unit_prop_name = self.get_unit_property_name(name)
        return self.get_default_value(node_name, unit_prop_name)

    def get_valid_values(self, node_name, name):
        prop = self.get_prop(node_name, name)
        if prop:
            return prop.get(ENUM, None)

    def get_valid_units(self, node_name, name):
        unit_prop_name = self.get_unit_property_name(name)
        return self.get_valid_values(node_name, unit_prop_name)

    def get_extra_props(self, node_name, name, value):
        results = {}
        prop = self.get_prop(node_name, name)
        if prop and HAS_UNIT in prop and prop[HAS_UNIT]:
            # For MVP use default unit for all values
            results[self.get_unit_property_name(name)] = self.get_default_unit(node_name, name)
            org_prop_name = self.get_original_value_property_name(name)
            # For MVP use value is same as original value
            results[org_prop_name] = value
            results[self.get_unit_property_name(org_prop_name)] = self.get_default_unit(node_name, name)
        return results

    def process_value_unit_type(self, name, prop_type):
        results = {}
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            if PROP_TYPE in prop:
                prop_desc = prop[PROP_TYPE]
                if isinstance(prop_desc, dict):
                    if UNITS in prop_desc:
                        units = prop_desc[UNITS]
                        if units:
                            enum = set(units)
                            unit_prop_name = self.get_unit_property_name(name)
                            results[unit_prop_name] = {PROP_TYPE: DEFAULT_TYPE, ENUM: enum, DEFAULT_VALUE: units[0]}
                            org_prop_name = self.get_original_value_property_name(name)
                            org_unit_prop_name = self.get_unit_property_name(org_prop_name)
                            results[org_prop_name] = prop_type
                            results[org_unit_prop_name] = {PROP_TYPE: DEFAULT_TYPE, ENUM: enum, DEFAULT_VALUE: units[0]}
        return results

    @staticmethod
    def get_unit_property_name(name):
        return name + '_unit'

    @staticmethod
    def get_original_value_property_name(name):
        return name + '_original'

    def validate_node(self, model_type, obj, verbose):
        result = {'result': True, 'messages': [], 'warning': False, 'invalid_values': [], 'invalid_properties': [], 'invalid_reason': [], 'missing_properties': [], 'missing_reason': []}
        if not model_type or model_type not in self.nodes:
            return {'result': False, 'messages': ['Node type: "{}" not found in data model'.format(model_type)], 'warning': False}
        if not obj:
            return {'result': False, 'messages': ['Node is empty!'], 'warning': False}

        if not isinstance(obj, dict):
            return {'result': False, 'messages': ['Node is not a dict!'], 'warning': False}

        # Make sure all required properties exist, and are not empty
        
        for prop in self.nodes[model_type].get(REQUIRED, set()):
            if prop not in obj:
                result['result'] = False
                result['messages'].append('Missing required property: "{}"!'.format(prop))
                result['missing_properties'].append(prop)
                result['missing_reason'].append('property_missing')
            elif not obj[prop]:
                result['result'] = False
                result['messages'].append('Required property: "{}" is empty!'.format(prop))
                result['missing_properties'].append(prop)
                result['missing_reason'].append('value_empty')

        properties = self.nodes[model_type][PROPERTIES]
        # Validate all properties in given object
        for key, value in obj.items():
            if key == NODE_TYPE:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.is_relationship_property(key):
                rel_type, rel_prop = key.split(self.rel_prop_delimiter)
                if rel_type not in self.relationship_props:
                    result['result'] = False
                    result['messages'].append(f'Relationship "{rel_type}" does NOT exist in data model!')
                    continue
                elif rel_prop not in self.relationship_props[rel_type][PROPERTIES]:
                    result['result'] = False
                    result['messages'].append(f'Property "{rel_prop}" does NOT exist in relationship "{rel_type}"!')
                    continue

                prop_type = self.relationship_props[rel_type][PROPERTIES][rel_prop]
                type_validation_result, error_type = self._validate_type(prop_type, value)
                if not type_validation_result:
                    result['result'] = False
                    result['invalid_values'].append(value)
                    result['invalid_properties'].append(rel_prop)
                    result['invalid_reason'].append(error_type)
                    if not verbose:
                        if error_type == "non_permissive_value":
                            result['messages'].append(
                                'Property: "{}":"{}" is not in permissible value list!'.format(rel_prop, value))
                        elif error_type == "wrong_type":
                            result['messages'].append(
                                'Property: "{}":"{}" is in wrong type!'.format(rel_prop, value))
                    else:
                        result['messages'].append(
                            'Property: "{}":"{}" is not a valid "{}" type!'.format(rel_prop, value, prop_type))

            elif key not in properties:
                self.log.debug('Property "{}" is not in data model!'.format(key))
            else:
                prop_type = properties[key]
                type_validation_result, error_type = self._validate_type(prop_type, value)
                if not type_validation_result:
                    if type(error_type) is tuple:
                        result['result'] = False
                        result['invalid_values'].append(error_type[0])
                        result['invalid_properties'].append(key)
                        result['invalid_reason'].append(error_type[1])
                        if not verbose:
                            if error_type[1] == "non_permissive_value":
                                result['messages'].append(
                                    'Property: "{}":"{}" is not in permissible value list!'.format(key, error_type[0]))
                            elif error_type[1] == "wrong_type":
                                result['messages'].append(
                                    'Property: "{}":"{}" is in wrong type!'.format(key, error_type[0]))
                        else:
                            result['messages'].append(
                                'Property: "{}":"{}" is not a valid "{}" type!'.format(key, error_type[0], prop_type))
                    else:
                        result['result'] = False
                        result['invalid_values'].append(value)
                        result['invalid_properties'].append(key)
                        result['invalid_reason'].append(error_type)
                        if not verbose:
                            if error_type == "non_permissive_value":
                                result['messages'].append(
                                    'Property: "{}":"{}" is not in permissible value list!'.format(key, value))
                            elif error_type == "wrong_type":
                                result['messages'].append(
                                    'Property: "{}":"{}" is in wrong type!'.format(key, value))
                        else:
                            result['messages'].append(
                                'Property: "{}":"{}" is not a valid "{}" type!'.format(key, value, prop_type))


        return result

    @staticmethod
    def _validate_value_range(model_type, value):
        """
        Validate an int of float value, return whether value is in range

        :param model_type: dict specify value type and boundary/range
        :param value: value to be validated
        :return: boolean
        """

        if MIN in model_type:
            if value < model_type[MIN]:
                return False
        if MAX in model_type:
            if value > model_type[MAX]:
                return False
        if EX_MIN in model_type:
            if value <= model_type[EX_MIN]:
                return False
        if EX_MAX in model_type:
            if value >= model_type[EX_MAX]:
                return False
        return True

    def _validate_type(self, model_type, str_value):
        wrong_type = "wrong_type"
        out_of_range = "out_of_range"
        non_permissive_value = "non_permissive_value"
        pass_type = "pass"
        if model_type[PROP_TYPE] == 'Float':
            try:
                if str_value:
                    value = float(str_value)
                    if not self._validate_value_range(model_type, value):
                        return False, out_of_range
            except ValueError:
                return False, wrong_type
        elif model_type[PROP_TYPE] == 'Int':
            try:
                if str_value:
                    value = int(str_value)
                    if not self._validate_value_range(model_type, value):
                        return False, out_of_range
            except ValueError:
                return False, wrong_type
        elif model_type[PROP_TYPE] == 'Boolean':
            if (str_value and not re.match(r'\byes\b|\btrue\b', str_value, re.IGNORECASE)
                    and not re.match(r'\bno\b|\bfalse\b', str_value, re.IGNORECASE)
                    and not re.match(r'\bltf\b', str_value, re.IGNORECASE)):
                return False, wrong_type
        elif model_type[PROP_TYPE] == 'Array':
            for item in self.get_list_values(str_value):
                if ENUM in model_type[ITEM_TYPE]:
                    validation_result, error_type = self._validate_type(model_type[ITEM_TYPE], item)
                    if not validation_result:
                        return False, (item, error_type)
                #validation_result, error_type = self._validate_type(model_type[ITEM_TYPE], item)
                #if not validation_result:
                #    return False, non_permissive_value

        elif model_type[PROP_TYPE] == 'Object':
            if not isinstance(str_value, dict):
                return False, wrong_type
        elif model_type[PROP_TYPE] == 'String':
            if ENUM in model_type:
                if not isinstance(str_value, str):
                    return False, wrong_type
                if str_value != '' and str_value not in model_type[ENUM]:
                    return False, non_permissive_value
        elif model_type[PROP_TYPE] == 'Date':
            if not isinstance(str_value, str):
                return False, wrong_type
            try:
                if str_value.strip() != '':
                    parse_date(str_value)
            except ValueError:
                return False, wrong_type
        elif model_type[PROP_TYPE] == 'DateTime':
            if not isinstance(str_value, str):
                return False, wrong_type
            try:
                if str_value.strip() != '':
                    parse_date(str_value)
            except ValueError:
                return False, wrong_type
        return True, pass_type

    # Find relationship type from src to dest
    def get_relationship(self, src, dest):
        if src in self.relationships:
            relationships = self.relationships[src]
            if relationships and dest in relationships:
                return relationships[dest]
            else:
                self.log.error('No relationships found for "{}"-->"{}"'.format(src, dest))
                return None
        else:
            self.log.debug('No relationships start from "{}"'.format(src))
            return None

    # Find destination node name from (:src)-[:name]->(:dest)
    def get_dest_node_for_relationship(self, src, name):
        if src in self.relationships:
            relationships = self.relationships[src]
            if relationships:
                for dest, rel in relationships.items():
                    if rel[RELATIONSHIP_TYPE] == name:
                        return dest
        else:
            self.log.error('Could not find any relationship from (:{})'.format(src))
        return None

    # Get type info from description
    def map_type(self, type_name):
        mapping = self.props.type_mapping
        result = DEFAULT_TYPE

        if type_name in mapping:
            result = mapping[type_name]
        else:
            self.log.debug('Type: "{}" has no mapping, use default type: "{}"'.format(type_name, DEFAULT_TYPE))

        return result

    def plural(self, word):
        plurals = self.props.plurals
        if word in plurals:
            return plurals[word]
        else:
            self.log.warning('Plural for "{}" not found!'.format(word))
            return 'NONE'

    # Get all node names, sorted
    def get_node_names(self):
        return sorted(self.nodes.keys())

    def node_count(self):
        return len(self.nodes)

    def relationship_count(self):
        return self.num_relationship

    # Get all properties of a node (name)
    def get_props_for_node(self, node_name):
        if node_name in self.nodes:
            return self.nodes[node_name][PROPERTIES]
        else:
            return None

    # Get all properties of a node (name)
    def get_public_props_for_node(self, node_name):
        if node_name in self.nodes:
            props = self.nodes[node_name][PROPERTIES].copy()
            for private_prop in self.nodes[node_name].get(PRIVATE, []):
                del (props[private_prop])
                self.log.info('Delete private property: "{}"'.format(private_prop))
            return props
        else:
            return None

    # Get node's id field, such as case_id for case node, or clinical_study_designation for study node
    def get_id_field(self, obj):
        if NODE_TYPE not in obj:
            self.log.error('get_id_field: there is no "{}" field in node, can\'t retrieve id!'.format(NODE_TYPE))
            return None
        node_type = obj[NODE_TYPE]
        id_fields = self.props.id_fields
        if node_type:
            return id_fields.get(node_type, 'uuid')
        else:
            self.log.error('get_id_field: "{}" field is empty'.format(NODE_TYPE))
            return None

    # Find node's id
    def get_id(self, obj):
        id_field = self.get_id_field(obj)
        if not id_field:
            return None
        if id_field not in obj:
            return None
        else:
            return obj[id_field]

    def is_relationship_property(self, key):
        return re.match('^.+\{}.+$'.format(self.rel_prop_delimiter), key)
