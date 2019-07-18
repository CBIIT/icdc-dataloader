import os
import yaml

NODES = 'Nodes'
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'String'
PROP_TYPE = 'Type'
END_POINTS = 'Ends'
SRC = 'Src'
DEST = 'Dst'
CUSTOM_QUERY_FILE = 'schema-queries.graphql'
VALUE_TYPE = 'value_type'
LABEL_NEXT = 'next'
MULTIPLIER = 'Mul'
NEXT_RELATIONSHIP = 'next'
DEFAULT_MULTIPLIER = 'many-to=one'

def plural(word):
    plurals = {
        'program': 'programs',
        'study': 'studies',
        'study_site': 'study_sites',
        'study_arm': 'study_arms',
        'agent': 'agents',
        'cohort': 'cohorts',
        'case': 'cases',
        'demographic': 'demographics',
        'cycle': 'cycles',
        'visit': 'visits',
        'principal_investigator': 'principal_investigators',
        'diagnosis': 'diagnoses',
        'enrollment': 'enrollments',
        'prior_therapy': 'prior_therapies',
        'prior_surgery': 'prior_surgeries',
        'agent_administration': 'agent_administrations',
        'sample': 'samples',
        'evaluation': 'evaluations',
        'assay': 'assays',
        'file': 'files',
        'image': 'images',
        'physical_exam': 'physical_exams',
        'vital_signs': 'vital_signs',
        'lab_exam': 'lab_exams',
        'adverse_event': 'adverse_events',
        'disease_extent': 'disease_extents',
        'follow_up': 'follow_ups',
        'off_study': 'off_studies',
        'off_treatment': 'off_treatments'
    }
    if word in plurals:
        return plurals[word]
    else:
        print('Plural for "{}" not found!'.format(word))
        return 'NONE'


class ICDC_Schema:
    def __init__(self, files):
        self.org_schema = {}
        for aFile in files:
            try:
                print('Reading schema file: {} ...'.format(aFile), end='')
                if os.path.isfile(aFile):
                    with open(aFile) as schema_file:
                        schema = yaml.safe_load(schema_file)
                        self.org_schema.update(schema)
                print('Done.')
            except Exception as e:
                print('')
                print(e)

        self.nodes = {}
        self.relationships = []
        self.numRelationships = 0

        for key, value in self.org_schema[NODES].items():
            # Assume all keys start with '_' are not regular nodes
            if not key.startswith('_'):
                self.process_node(key, value)
        # print("-------------processing edges-----------------")
        for key, value in self.org_schema[RELATIONSHIPS].items():
            # Assume all keys start with '_' are not regular nodes
            if not key.startswith('_'):
                self.numRelationships += self.process_edges(key, value)

    def process_node(self, name, desc):
        # Gather properties
        props = {}
        if desc[PROPERTIES]:
            for prop in desc[PROPERTIES]:
                prop_type = self.get_type(prop)
                props[prop] = prop_type

        self.nodes[name] = props

    def process_edges(self, name, desc):
        # for key, value in desc.items():
        #     if key != END_POINTS:
        #         print('{}: {}'.format(key, value))
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
                    print('End point multiplier: "{}" overriding relationship multiplier: "{}"'.format(actual_multiplier, multiplier))
                else:
                    actual_multiplier = multiplier

                count += 1
                # print('{} -[:{}]-> {}'.format(src, name, dest))
                if src in self.nodes:
                    self.add_relationship_to_node(src, actual_multiplier, name, dest)
                    # nodes[src][plural(dest)] = '[{}] @relation(name:"{}")'.format(dest, name)
                else:
                    print('Source node "{}" not found!'.format(src))
                if dest in self.nodes:
                    self.add_relationship_to_node(dest, actual_multiplier, name, src, True)
                    # nodes[dest][plural(src)] = '[{}] @relation(name:"{}", direction:IN)'.format(src, name)
                else:
                    print('Destination node "{}" not found!'.format(dest))
        return count

    # Process singular/plural array/single value based on relationship multipliers like  many-to-many, many-to-one etc.
    # Return a relationship property to add into a node
    def add_relationship_to_node(self, name, multiplier, relationship, otherNode, dest=False):
        node = self.nodes[name]
        if multiplier == 'many_to_one':
            if dest:
                node[plural(otherNode)] = '[{}] @relation(name:"{}", direction:IN)'.format(otherNode, relationship)
            else:
                node[otherNode] = '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship)
        elif multiplier == 'one_to_one':
            if relationship == NEXT_RELATIONSHIP:
                if dest:
                    node['prior_' + otherNode] = '{} @relation(name:"{}", direction:IN)'.format(otherNode, relationship)
                else:
                    node['next_' + otherNode] = '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship)
            else:
                if dest:
                    node[otherNode] = '{} @relation(name:"{}", direction:IN)'.format(otherNode, relationship)
                else:
                    node[otherNode] = '{} @relation(name:"{}", direction:OUT)'.format(otherNode, relationship)
        elif multiplier == 'many_to_many':
            if dest:
                node[plural(otherNode)] = '[{}] @relation(name:"{}", direction:IN)'.format(otherNode, relationship)
            else:
                node[plural(otherNode)] = '[{}] @relation(name:"{}", direction:OUT)'.format(otherNode, relationship)
        else:
            print('Unsupported relationship multiplier: "{}"'.format(multiplier))

    def get_type(self, name):
        result = DEFAULT_TYPE
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            if PROP_TYPE in prop:
                prop_desc = prop[PROP_TYPE]
                if type(prop_desc) is str:
                    result = self.map_type(prop_desc)
                elif type(prop_desc) is dict:
                    if VALUE_TYPE in prop_desc:
                        result = self.map_type(prop_desc[VALUE_TYPE])
                else:
                    print('Property type: "{}" not supported, use default type: "{}"'.format(prop_desc, DEFAULT_TYPE))

        return result

    # Get type info from description
    def map_type(self, type_name):
        mapping = {
            'string': 'String',
            'number': 'Float',
            'integer': 'Int',
            'boolean': 'Boolean',
            'array': 'Array',
            'object': 'Object',
            'datetime': 'String',
            'TBD': 'String'
        }
        result = DEFAULT_TYPE

        if type_name in mapping:
            result = mapping[type_name]
        else:
            print('Type: "{}" has no mapping, use default type: "{}"'.format(type_name, DEFAULT_TYPE))

        return result

if __name__ == '__main__':
    files = ['/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model.yml', '/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model-props.yml']

    schema = ICDC_Schema(files)
    for key in schema.org_schema:
        print(key)