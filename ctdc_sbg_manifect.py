import argparse
import csv
import os

from neo4j import GraphDatabase

from bento.common.utils import get_logger, get_time_stamp, LOG_PREFIX

PSWD_ENV = 'NEO_PASSWORD'
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'CTDC_SBG_Manifest'

SPECIMEN_ID = 'Specimen_ID'

def generate(tx, log):
    query = '''
    MATCH(t:clinical_trial)<--(a:arm)<--(c:case)<--(s:specimen)<--(n:nucleic_acid)<--(sa:sequencing_assay)<--(f:file),
     (s)<-[*]-(ar:assignment_report), (sa)<--(v:variant_report)
    WITH DISTINCT f, t, a, c, ar, s, n, sa, v
    OPTIONAL MATCH (s)<--(i_pten:ihc_assay_report)
      WHERE i_pten.ihc_test_gene = 'PTEN'
    WITH DISTINCT f, t, a, c, ar, s, n, sa, v, i_pten
    OPTIONAL MATCH (s)<--(i_msh2:ihc_assay_report)
      WHERE i_msh2.ihc_test_gene = 'MSH2'
    WITH DISTINCT f, t, a, c, ar, s, n, sa, v, i_pten, i_msh2
    OPTIONAL MATCH (s)<--(i_mlh1:ihc_assay_report)
      WHERE i_mlh1.ihc_test_gene = 'MLH1'
    WITH DISTINCT f, t, a, c, ar, s, n, sa, v, i_pten, i_msh2, i_mlh1
    OPTIONAL MATCH (s)<--(i_rb:ihc_assay_report)
      WHERE i_rb.ihc_test_gene = 'RB'
    WITH DISTINCT f, t, a, c, ar, s, n, sa, v, i_pten, i_msh2, i_mlh1, i_rb
    RETURN t.clinical_trial_id AS Trial_ID, t.clinical_trial_designation AS Trial_Code,
       a.arm_id AS Treatment_Arm,
       c.case_id AS Case_ID, c.gender AS Gender, c.race AS Race, c.ethnicity AS Ethnicity, c.disease AS Diagnosis,
       c.ctep_category AS CTEP_Category, c.ctep_subcategory AS CTEP_Sub_Category, c.meddra_code AS MedDRA_Code,
       c.prior_drugs AS Prior_Drugs,
       s.specimen_id AS Specimen_ID,
       s.specimen_type AS Specimen_Type,
       n.aliquot_id AS Aliquot_ID,
       coalesce(i_pten.ihc_test_result, 'UNKNOWN') AS PTEN_IHC_Status,
       coalesce(i_mlh1.ihc_test_result, 'UNKNOWN') AS MLH1_IHC_Status,
       coalesce(i_msh2.ihc_test_result, 'UNKNOWN') AS MSH2_IHC_Status,
       coalesce(i_rb.ihc_test_result, 'UNKNOWN') AS RB_IHC_Status,
       ar.assignment_outcome AS Assignment_Outcome,
       sa.experimental_method + ':' + CASE f.file_type
         WHEN 'Aligned DNA reads file' THEN ' DNA'
         WHEN 'Aligned RNA reads file' THEN ' RNA'
         WHEN 'Variants file' THEN ' DNA/RNA'
         WHEN 'Index file' THEN ' '
         END AS `Experimental_strategy`,
       sa.platform AS Platform,
       v.reference_genome AS Reference_genome,
       f.uuid AS File_UUID, f.file_name AS File_Name, f.file_type AS File_Type, f.file_size AS File_Size,
       f.md5sum AS md5sum, f.file_location AS File_Location, 'dg.4DFC/' + f.uuid AS GUID
      ORDER BY Treatment_Arm, Case_ID, File_Name
    '''

    fieldnames = [
        "Trial_ID",
        "Trial_Code",
        "Treatment_Arm",
        "Case_ID",
        "Gender",
        "Race",
        "Ethnicity",
        "Diagnosis",
        "CTEP_Category",
        "CTEP_Sub_Category",
        "MedDRA_Code",
        "Prior_Drugs",
        "Specimen_ID",
        "Specimen_Type",
        "Aliquot_ID",
        "PTEN_IHC_Status",
        "MLH1_IHC_Status",
        "MSH2_IHC_Status",
        "RB_IHC_Status",
        "Assignment_Outcome",
        "Experimental_strategy",
        "Platform",
        "Reference_genome",
        "File_UUID",
        "File_Name",
        "File_Type",
        "File_Size",
        "md5sum",
        "File_Location",
        "GUID"
    ]

    result = tx.run(query)
    manifest_file = f'tmp/CTDC_SBG_Manifest_{get_time_stamp()}.csv'

    with open(manifest_file, 'w') as of:
        writer = csv.DictWriter(of, fieldnames=fieldnames)
        writer.writeheader()
        specimen_list = {}
        file_list = []
        line_num = 1
        for obj in result:
            line_num += 1
            file_name = obj['File_Name']
            specimen_id = obj[SPECIMEN_ID]
            log.info(f'Processing {obj["Case_ID"]}: {specimen_id}: {file_name}')
            specimen = specimen_list.get(specimen_id, {})
            if file_name in specimen:
                raise Exception(f'Line: {line_num} - Duplicated file name: "{file_name}"')

            data = obj.data()
            specimen[file_name] = data
            file_list.append(data)
            specimen_list[specimen_id] = specimen

        for file in file_list:
            specimen = specimen_list[file[SPECIMEN_ID]]
            if file['File_Type'] == 'Index file':
                update_experimental_strategy(file, specimen)

            log.info(f'Saving {file["Case_ID"]}: {file["File_Name"]}')
            writer.writerow(file)

    log.info(f'Manifest saved to "{manifest_file}"')


def update_experimental_strategy(file, specimen):
    file_name = file['File_Name']
    for name, obj in specimen.items():
        if name == file_name:
            continue
        elif file_name.startswith(name):
            file['Experimental_strategy'] = obj['Experimental_strategy']
            return

def main():
    parser = argparse.ArgumentParser(description='Generate CTDC SBG manifest')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user', default='neo4j')
    parser.add_argument('-p', '--password', help='Neo4j password', default=os.environ[PSWD_ENV])
    args = parser.parse_args()

    log = get_logger('CTDC_SBG_Manifest')

    with GraphDatabase.driver(args.uri, auth=(args.user, args.password)) as driver:
        with driver.session() as session:
            tx = session.begin_transaction()
            generate(tx, log)


if __name__ == '__main__':
    main()