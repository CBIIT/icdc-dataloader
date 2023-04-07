from neo4j import GraphDatabase
uri = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "12341234"))

def import_dir(driver):
    with driver.session() as session:
        records, summary= session.execute_read(import_dir_query)
    return records[0].data()['value'].replace(' ','\ ')

def import_dir_query(tx):  
    result = tx.run(
        "Call dbms.listConfig() YIELD name, value WHERE name='dbms.directories.neo4j_home' RETURN value"
    )
    records = list(result)
    summary = result.consume()
    # print(summary)
    return records, summary

print(import_dir(driver))

f1= open('/Users/lauwc/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-6e7e98ab-2838-4e0f-9f14-907e63fbb571/import/temp.txt','w')
f1.close()