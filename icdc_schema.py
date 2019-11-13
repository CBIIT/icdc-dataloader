import os
import yaml
import sys
from utils import *
import re
from datetime import datetime

NODES = 'Nodes'
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'String'
PROP_TYPE = 'Type'
END_POINTS = 'Ends'
SRC = 'Src'
DEST = 'Dst'
VALUE_TYPE = 'value_type'
LABEL_NEXT = 'next'
NEXT_RELATIONSHIP = 'next'
UNITS = 'units'
REQUIRED = 'Req'
NODE_TYPE = 'type'
ENUM = 'enum'
DEFAULT_VALUE = 'default_value'
HAS_UNIT = 'has_unit'

class ICDC_Schema:
    def __init__(self, files):
        if not files:
            raise Exception('File list is empty, couldn\'t initialize ICDC_Schema object!')
        else:
            for data_file in files:
                if not os.path.isfile(data_file):
                    raise Exception('File "{}" doesn\'t exist'.format(data_file))
        self.log = get_logger('ICDC Schema')
        self.org_schema = {}
        for aFile in files:
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
                    self.num_relationship += self.process_edges(key, value)


    def process_node(self, name, desc):
        # Gather properties
        props = {}
        required = set()
        if desc[PROPERTIES]:
            for prop in desc[PROPERTIES]:
                prop_type = self.get_type(prop)
                props[prop] = prop_type
                value_unit_props = self.process_value_unit_type(prop, prop_type)
                if value_unit_props:
                    props.update(value_unit_props)
                if self.is_required_prop(prop):
                    required.add(prop)

        self.nodes[name] = { PROPERTIES: props, REQUIRED: required }

    def process_edges(self, name, desc):
        count = 0
        if MULTIPLIER in desc:
            multiplier = desc[MULTIPLIER]
        else:
            multiplier = DEFAULT_MULTIPLIER

        if END_POINTS in desc:
            for  end_points in desc[END_POINTS]:
                src = end_points[SRC]
                dest = end_points[DEST]
                if MULTIPLIER in end_points:
                    actual_multiplier = end_points[MULTIPLIER]
                    self.log.debug('End point multiplier: "{}" overriding relationship multiplier: "{}"'.format(actual_multiplier, multiplier))
                else:
                    actual_multiplier = multiplier
                if src not in self.relationships:
                    self.relationships[src] = {}
                self.relationships[src][dest] = { RELATIONSHIP_TYPE: name, MULTIPLIER: actual_multiplier }

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
    def add_relationship_to_node(self, name, multiplier, relationship, otherNode, dest=False):
        node = self.nodes[name]
        if multiplier == 'many_to_one':
            if dest:
                node[PROPERTIES][self.plural(otherNode)] = { PROP_TYPE: '[{}] @relation(name:"{}", direction:IN)'.format(otherNode, relationship) }
            else:
                node[PROPERTIES][otherNode] = {PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship) }
        elif multiplier == 'one_to_one':
            if relationship == NEXT_RELATIONSHIP:
                if dest:
                    node[PROPERTIES]['prior_' + otherNode] = {PROP_TYPE: '{} @relation(name:"{}", direction:IN)'.format(otherNode, relationship) }
                else:
                    node[PROPERTIES]['next_' + otherNode] = {PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship) }
            else:
                if dest:
                    node[PROPERTIES][otherNode] = {PROP_TYPE: '{} @relation(name:"{}", direction:IN)'.format(otherNode, relationship) }
                else:
                    node[PROPERTIES][otherNode] = {PROP_TYPE: '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship) }
        elif multiplier == 'many_to_many':
            if dest:
                node[PROPERTIES][self.plural(otherNode)] = {PROP_TYPE: '[{}] @relation(name:"{}", direction:IN)'.format(otherNode, relationship) }
            else:
                node[PROPERTIES][self.plural(otherNode)] = {PROP_TYPE: '[{}] @relation(name:"{}", direction:OUT)'.format(otherNode, relationship) }
        else:
            self.log.warning('Unsupported relationship multiplier: "{}"'.format(multiplier))

    def is_required_prop(self, name):
        result = False
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result = prop.get(REQUIRED, False)
        return result

    def get_prop_type(self, node_type, prop):
        if node_type in  self.nodes:
            node = self.nodes[node_type]
            if prop in node[PROPERTIES]:
                return node[PROPERTIES][prop][PROP_TYPE]
        return DEFAULT_TYPE

    def get_type(self, name):
        result = { PROP_TYPE: DEFAULT_TYPE }
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            if PROP_TYPE in prop:
                prop_desc = prop[PROP_TYPE]
                if isinstance(prop_desc, str):
                    result[PROP_TYPE] = self.map_type(prop_desc)
                elif isinstance(prop_desc, dict):
                    if VALUE_TYPE in prop_desc:
                        result[PROP_TYPE] = self.map_type(prop_desc[VALUE_TYPE])
                        if UNITS in prop_desc:
                            result[HAS_UNIT] = True
                elif isinstance(prop_desc, list):
                    enum = set()
                    for t in prop_desc:
                        if not re.search(r'://', t):
                            enum.add(t)
                    if len(enum) > 0:
                        result[ENUM] = enum
                else:
                    self.log.debug('Property type: "{}" not supported, use default type: "{}"'.format(prop_desc, DEFAULT_TYPE))

        return result

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
                            results[unit_prop_name] =  {PROP_TYPE: DEFAULT_TYPE, ENUM: enum, DEFAULT_VALUE: units[0]}
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

    def validate_node(self, model_type, obj):
        if not model_type or model_type not in self.nodes:
            return {'result': False, 'messages': ['Node type: "{}" doesn\'t exist!'.format(model_type)]}
        if not obj:
            return {'result': False, 'messages': ['Node is empty!']}

        if not isinstance(obj, dict):
            return {'result': False, 'messages': ['Node is not a dict!']}

        # Make sure all required properties exist, and are not empty
        result = {'result': True, 'messages': []}
        for prop in self.nodes[model_type].get(REQUIRED, set()):
            if prop not in obj:
                result['result'] = False
                result['messages'].append('Missing required property: "{}"!'.format(prop))
            elif not obj[prop]:
                result['result'] = False
                result['messages'].append('Required property: "{}" is empty!'.format(prop))

        properties = self.nodes[model_type][PROPERTIES]
        # Validate all properties in given object
        for key, value in obj.items():
            if key == NODE_TYPE:
                continue
            elif is_parent_pointer(key):
                continue
            elif key not in properties:
                self.log.debug('Property "{}" is not in data model!'.format(key))
            else:
                model_type = properties[key]
                if not self.valid_type(model_type, value):
                    result['result'] = False
                    result['messages'].append('Property: "{}":"{}" is not a valid "{}" type!'.format(key, value, model_type))

        return result

    @staticmethod
    def valid_type(model_type, value):
        if model_type[PROP_TYPE] == 'Float':
            try:
                if value:
                    _ = float(value)
            except ValueError:
                return False
        elif model_type[PROP_TYPE] == 'Int':
            try:
                if value:
                    _ = int(value)
            except ValueError:
                return False
        elif model_type[PROP_TYPE] == 'Boolean':
            if value and not re.match(r'\byes\b|\btrue\b', value, re.IGNORECASE) and not re.match(r'\bno\b|\bfalse\b', value, re.IGNORECASE) and not re.match(r'\bltf\b', value, re.IGNORECASE):
                return False
        elif model_type[PROP_TYPE] == 'Array':
            if not isinstance(value, list):
                return False
        elif model_type[PROP_TYPE] == 'Object':
            if not isinstance(value, dict):
                return False
        elif model_type[PROP_TYPE] == 'String':
            if  ENUM in model_type:
                if not isinstance(value, str):
                    return False
                if not value in model_type[ENUM]:
                    return False
        elif model_type[PROP_TYPE] == 'Date':
            if not isinstance(value, str):
                return False
            try:
                if value.strip() != '':
                    _ = datetime.strptime(value, DATE_FORMAT)
            except ValueError:
                return False
        elif model_type[PROP_TYPE] == 'DateTime':
            if not isinstance(value, str):
                return False
            try:
                if value.strip() != '':
                    _ = datetime.strptime(value, DATE_FORMAT)
            except ValueError:
                return False
        return True

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
            self.log.error('Couldn\'t find any relationship from (:{})'.format(src))
        return None


    # Get type info from description
    def map_type(self, type_name):
        mapping = PROPS['type_mapping']
        result = DEFAULT_TYPE

        if type_name in mapping:
            result = mapping[type_name]
        else:
            self.log.debug('Type: "{}" has no mapping, use default type: "{}"'.format(type_name, DEFAULT_TYPE))

        return result

    def plural(self, word):
        plurals = PROPS['plurals']
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

if __name__ == '__main__':
    files = ['/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model.yml', '/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model-props.yml']

    schema = ICDC_Schema(files)
    for key in schema.org_schema:
        print(key)