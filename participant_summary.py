# -*- coding: utf-8 -*-
"""
Created on Thu Jul 24 07:55:03 2025

@author: breadsp2
"""
from neo4j import GraphDatabase
from config import BentoConfig


def get_study_summary(tx):
    neo4j_query = ""
    neo4j_query += "MATCH (s:study)-[r]-(p:participant) "
    neo4j_query += "RETURN s.study_short_name as study_short_name, "
    neo4j_query += "toInteger(round(max(p.age_at_enrollment / 365.25)))  as participant_maximum_age, "
    neo4j_query += "toInteger(round(apoc.agg.median(p.age_at_enrollment / 365.25))) as participant_median_age, "
    neo4j_query += "toInteger(round(avg(p.age_at_enrollment / 365.25))) as participant_mean_age, "
    neo4j_query += "toInteger(round(min(p.age_at_enrollment / 365.25))) as participant_minimum_age, "
    neo4j_query += "toInteger(s.study_beginning_year) + ' - ' + COALESCE(s.study_ending_year,'On Going') as study_period, "
    neo4j_query += "count(distinct(p)) as number_of_participants,"
    neo4j_query += "collect(distinct p.cancer_diagnosis_primary_site) as cancer_diagnosis_primary_site_list"
    result = tx.run(neo4j_query)
    data_list = [i for i in result.data()]
    return data_list


def get_study_data(tx):
    neo4j_query = "MATCH (s:study)-[r]-(p:participant) return s"
    result = tx.run(neo4j_query)
    data_list = [i for i in result.data()]
    return data_list


def process_data_in_batches(tx, data, node_type):
    new_qry = """CALL apoc.periodic.iterate( """
    new_qry += """\"UNWIND $data AS item return item\", """  # Iterate statement: Unwinds the list of data items

    new_qry += """ \"Match(n:MyNode) where """ + "n.study_short_name = item.study_short_name """
    new_qry += """  SET n += item,  n.updated = datetime()  return n \", """

    new_qry += """{batchSize: 10000, retries: 1, """   # Process 1000 items per batch
    new_qry += """parallel: true, """    # Run batches sequentially
    new_qry += """params: { data: $data } } )"""
    new_qry = new_qry.replace("MyNode", node_type)

    result = tx.run(new_qry, data=data)
    data_list = [i for i in result.data()]
    if data_list[0]["failedBatches"] == 0:
        return data_list[0]
    else:
        print("error found")
        return []


def main():
    config_file = './config/popsci-config_v2.yml'
    config = BentoConfig(config_file)
    driver = GraphDatabase.driver(config.neo4j_uri, auth=(config.neo4j_user, config.neo4j_password), encrypted=False)

    with driver.session() as session:
        # old_data = session.read_transaction(get_study_data)
        records = session.read_transaction(get_study_summary)
        qry_result = session.write_transaction(process_data_in_batches, records, "study")

        # records is a list of dictionarys
        # each dictionary has results of the query


main()