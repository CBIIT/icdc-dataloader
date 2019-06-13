#!/usr/bin/env python3

from neo4j import GraphDatabase
import os
import csv
import json
import argparse

edge_tables = [
        ['edge_adverseeventatevaluationevaluation', 'at_evaluation'],
        ['edge_adverseeventhadadverseeventcase', 'had_adverse_event'],
        ['edge_adverseeventnextadverseevent', 'next'],
        ['edge_adverseeventofagentagent', 'of_agent'],
        ['edge_adverseeventrelatestocase', 'relates_to'],
        ['edge_agentadministrationofagentagent', 'of_agent'],
        ['edge_agentadministrationonvisitvisit', 'on_visit'],
        ['edge_agentofstudyarmstudyarm', 'of_study_arm'],
        ['edge_aliquotmemberofsample', 'member_of'],
        ['edge_aliquotrelatestocase', 'relates_to'],
        ['edge_assayofsamplesample', 'of_sample'],
        ['edge_assayrelatestocase', 'relates_to'],
        ['edge_casememberofcohort', 'member_of'],
        ['edge_cohortmemberofexperiment', 'member_of'],
        ['edge_cohortmemberofstudyarm', 'member_of'],
        ['edge_cycleofcasecase', 'of_case'],
        ['edge_datareleasedescribesroot', 'describes'],
        ['edge_datareleaserelatestocase', 'relates_to'],
        ['edge_demographicofcasecase', 'of_case'],
        ['edge_demographicrelatestocase', 'relates_to'],
        ['edge_diagnosisofcasecase', 'of_case'],
        ['edge_diagnosisrelatestocase', 'relates_to'],
        ['edge_diseaseextentatevaluationevaluation', 'at_evaluation'],
        ['edge_diseaseextentrelatestocase', 'relates_to'],
        ['edge_enrollmentofcasecase', 'of_case'],
        ['edge_evaluationonvisitvisit', 'on_visit'],
        ['edge_evaluationrelatestocase', 'relates_to'],
        ['edge_experimentmemberofproject', 'member_of'],
        ['edge_experimentmemberofstudyarm', 'member_of'],
        ['edge_fileofassayassay', 'of_assay'],
        ['edge_filerelatestocase', 'relates_to'],
        ['edge_followupofcasecase', 'of_case'],
        ['edge_followuprelatestocase', 'relates_to'],
        ['edge_imageofassayassay', 'of_assay'],
        ['edge_imagerelatestocase', 'relates_to'],
        ['edge_labexamatevaluationevaluation', 'at_evaluation'],
        ['edge_labexamrelatestocase', 'relates_to'],
        ['edge_offstudyrelatestocase', 'relates_to'],
        ['edge_offstudywentoffstudycase', 'went_off_study'],
        ['edge_offtreatmentrelatestocase', 'relates_to'],
        ['edge_offtreatmentwentofftreatmentcase', 'went_off_treatment'],
        ['edge_physicalexamatenrollmentenrollment', 'at_enrollment'],
        ['edge_physicalexamatevaluationevaluation', 'at_evaluation'],
        ['edge_physicalexamrelatestocase', 'relates_to'],
        ['edge_principalinvestigatorofstudystudy', 'of_study'],
        ['edge_priorsurgeryatenrollmentenrollment', 'at_enrollment'],
        ['edge_priorsurgerynextpriorsurgery', 'next'],
        ['edge_priorsurgeryrelatestocase', 'relates_to'],
        ['edge_priortherapyatenrollmentenrollment', 'at_enrollment'],
        ['edge_priortherapynextpriortherapy', 'next'],
        ['edge_priortherapyrelatestocase', 'relates_to'],
        ['edge_projectmemberofprogram', 'member_of'],
        ['edge_rootrelatestocase', 'relates_to'],
        ['edge_samplememberofcase', 'member_of'],
        ['edge_samplenextsample', 'next'],
        ['edge_sampleonvisitvisit', 'on_visit'],
        ['edge_samplerelatestocase', 'relates_to'],
        ['edge_studyarmmemberofstudy', 'member_of'],
        ['edge_studymemberofproject', 'member_of'],
        ['edge_studysiteofstudystudy', 'of_study'],
        ['edge_visitnextvisit', 'next'],
        ['edge_visitofcyclecycle', 'of_cycle'],
        ['edge_visitrelatestocase', 'relates_to'],
        ['edge_vitalsignsatevaluationevaluation', 'at_evaluation'],
        ['edge_vitalsignsrelatestocase', 'relates_to']
        ]

node_tables = [
    ['node_adverseevent', 'adverse_event'],
    ['node_agent','agent'],
    ['node_agentadministration','agent_administration'],
    ['node_aliquot','aliquot'],
    ['node_assay','assay'],
    ['node_case','case'],
    ['node_cohort','cohort'],
    ['node_cycle','cycle'],
    ['node_demographic','demographic'],
    ['node_diagnosis','diagnosis'],
    ['node_diseaseextent','disease_extent'],
    ['node_enrollment','enrollment'],
    ['node_evaluation','evaluation'],
    ['node_experiment','experiment'],
    ['node_file','file'],
    ['node_followup','follow_up'],
    ['node_image','image'],
    ['node_labexam','lab_exam'],
    ['node_offstudy','off_study'],
    ['node_offtreatment','off_treatment'],
    ['node_physicalexam','physical_exam'],
    ['node_principalinvestigator','principal_investigator'],
    ['node_priorsurgery','prior_surgery'],
    ['node_priortherapy','prior_therapy'],
    ['node_program','program'],
    ['node_project','project'],
    ['node_sample','sample'],
    ['node_study','study'],
    ['node_studyarm','study_arm'],
    ['node_studysite','study_site'],
    ['node_visit','visit'],
    ['node_vitalsigns', 'vital_signs'],
    ['node_root','root'],
    ['node_datarelease','data_release']
        ]

other_tables = [
    '_voided_edges',
    '_voided_nodes',
    'transaction_documents',
    'transaction_logs',
    'transaction_snapshots'
]

parser = argparse.ArgumentParser(description='Import GEN3 PostgreSQL database in csv files into Neo4j')
parser.add_argument('dir', help='Data directory')
parser.add_argument('-u', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')

args = parser.parse_args()

uri = args.uri if args.uri else "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", os.environ['NEO_PASSWORD']))


with driver.session() as session:
    for table in node_tables:
        # insert nodes
        if table[0].startswith('node_'):
            #continue
            print(table[0])
            # label = table[0].replace('node_', '')
            label = table[1]
            with open('{}/{}.csv'.format(args.dir, table[0])) as inf:
                reader = csv.DictReader(inf)
                for row in reader:
                    props = json.loads(row['_props'])
                    prop_statement = 'SET n.created = "{}", n.acl = "{}", n._sysan = "{}"'.format(row['created'], row['acl'], row['_sysan'])
                    if props:
                        for key, val in props.items():
                            prop_statement += ', n.{} = "{}"'.format(key, val)
                    statement = 'MERGE (n:{0} {{id: "{1}"}}) on create {2} on match {2}'.format(label, row['node_id'], prop_statement)
                    # print(statement)
                    print(session.run(statement))
    for table in edge_tables:
        # insert edges
        if table[0].startswith('edge_'):
            print(table[0])
            # label = table[0].replace('edge_', '')
            label = table[1]
            with open('{}/{}.csv'.format(args.dir, table[0])) as inf:
                reader = csv.DictReader(inf)
                for row in reader:
                    props = json.loads(row['_props'])
                    prop_statement = 'SET n.created = "{}", n.acl = "{}", n._sysan = "{}"'.format(row['created'], row['acl'], row['_sysan'])
                    if props:
                        for key, val in props.items():
                            prop_statement += ', n.{} = "{}"'.format(key, val)
                    statement = 'MATCH (n1 {{id: "{0}"}}), (n2 {{id: "{1}"}}) MERGE (n1)-[n:{2}]->(n2) on create {3} on match {3}'.format(row['src_id'], row['dst_id'], label, prop_statement)
                    # print(statement)
                    print(session.run(statement))
