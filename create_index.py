NEO4J = "neo4j"
MEMGRAPH = "memgraph"
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

def create_index(driver, schema, log, database_type):
    index_created = 0
    if database_type == NEO4J:
        with driver.session() as session:
            tx = session.begin_transaction()
            try:
                index_created = create_indexes(tx, schema, log, database_type)
                tx.commit()
            except Exception as e:
                tx.rollback()
                log.exception(e)
                return False
    elif database_type == MEMGRAPH:
        try:
            #cursor = driver.cursor()
            with driver.session() as session:
                index_created = create_indexes(session, schema, log, database_type)
        except Exception as e:
            log.exception(e)
            return False
    return index_created

def get_memgraph_index_info(session):
    #query the memgraph database to get all existing indexes
    command = "SHOW INDEX INFO"
    result = session.run(command)
    indexes = set()
    for r in result:
        indexes.add(format_as_tuple(r["label"], r["property"]))
    return indexes

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

def create_indexes(session, schema, log, database_type):
    """
    Creates indexes, if they do not already exist, for all entries in the "id_fields" and "indexes" sections of the
    properties file
    :param session: the current neo4j transaction session
    """
    index_created = 0
    if database_type == NEO4J:
        existing = get_btree_indexes(session)
    elif database_type == MEMGRAPH:
        existing = get_memgraph_index_info(session)
    # Create indexes from "id_fields" section of the properties file
    ids = schema.props.id_fields
    for node_name in ids:
        index_created = add_index(node_name, ids[node_name], existing, session, log, index_created)
    # Create indexes from "indexes" section of the properties file
    indexes = schema.props.indexes
    # each index is a dictionary, indexes is a list of these dictionaries
    # for each dictionary in list
    for node_dict in indexes:
        node_name = list(node_dict.keys())[0]
        index_created = add_index(node_name, node_dict[node_name], existing, session, log, index_created)

    return index_created

def add_index(node_name, node_property, existing, session, log, index_created):
    index_tuple = format_as_tuple(node_name, node_property)
    # If node_property is a list of properties, convert to a comma delimited string
    if isinstance(node_property, list):
        node_property = ",".join(node_property)
    if index_tuple not in existing:
        command = "CREATE INDEX ON :{}({});".format(node_name, node_property)
        session.run(command)
        index_created += 1
        log.info("Index created for \"{}\" on property \"{}\"".format(node_name, node_property))
    return index_created
