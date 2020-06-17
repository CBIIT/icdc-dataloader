// Example 1
:params {study_name: "Preclinical Evaluation of Three Idenoisoquinoline Candidates in Tumor-Bearing Dogs"};
MATCH (s:study)<-[:member_of]-(a:study_arm)<-[:member_of]-(c:cohort)<-[:member_of]-(case:case)<-[:of_case]-(d:demographic),
      (case)<-[:of_case]-(diag:diagnosis) 
WHERE s.clinical_study_name = $study_name
RETURN case.patient_id AS patient_id,
       d.breed AS breed,
       d.patient_age_at_enrollment AS patient_age_at_enrollment,
       d.sex AS sex,
       diag.disease_term AS disease_term,
       diag.stage_of_disease AS stage_of_disease

// Example 2
MATCH (case:case)<-[:of_case]-(d:demographic)
RETURN d.breed AS breed, count(case) AS `count of cases`
ORDER BY d.breed

// Example 3
MATCH (diag:diagnosis)-[:of_case]->(case:case)
RETURN diag.disease_term AS disease_term, count(case) AS `count of cases`
ORDER BY diag.disease_term

// Example 4
:params {study_name: "Preclinical Evaluation of Three Idenoisoquinoline Candidates in Tumor-Bearing Dogs", breeds: ["Golden Retriever", "Labrador Retriever", "Beagle", "Mixed Breed"], disease_terms: ["Lymphoma", "Malignant lymphoma"] }
MATCH (s:study)<-[:member_of]-(a:study_arm)<-[:member_of]-(c:cohort)<-[:member_of]-(case:case)<-[:of_case]-(d:demographic),
      (case)<-[:of_case]-(diag:diagnosis) 
WHERE s.clinical_study_name = $study_name
      AND d.breed IN $breeds
      AND diag.disease_term IN $disease_terms
RETURN case.patient_id AS patient_id,
       d.breed AS breed,
       d.patient_age_at_enrollment AS `patient_age_at_enrollment`,
       d.sex AS sex,
       diag.disease_term AS disease_term,
       diag.stage_of_disease AS stage_of_disease


// Export 4 nodes
MATCH (s:study)<-[*]-(c:case)<-[:of_case]-(e:enrollment),
      (c)<-[:of_case]-(diag:diagnosis),
      (c)<-[:of_case]-(demo:demographic)
RETURN s.submitter_id AS study_code, c.patient_first_name AS patient_first_name, c.patient_id AS patient_id, c.project_id AS project_id,
      demo.breed AS breed, demo.date_of_birth AS date_of_birth, demo.patient_age_at_enrollment AS patient_age_at_enrollment, demo.sex AS sex,
      e.initials AS initials, e.enrollment_document_number AS enrollment_document_number, e.date_of_registration AS date_of_registration, e.cohort_description AS cohort_description, e.date_of_informed_consent AS date_of_informed_consent,
      e.site_short_name AS site_short_name, e.veterinary_medical_center AS veterinary_medical_center, e.patient_subgroup AS patient_subgroup,
      diag.histology_cytopathology AS histology_cytopathology, diag.date_of_histology_confirmation AS date_of_histology_confirmation, diag.date_of_diagnosis AS date_of_diagnosis, diag.primary_disease_site AS primary_disease_site,
      diag.stage_of_disease AS stage_of_disease, diag.disease_term AS disease_term, diag.histological_grade AS histological_grade

// Summary View
MATCH (p:program)<--(s:study)<--(a:study_arm)<--(co:cohort)
RETURN p, s, a, co

// Detailed View
MATCH (p:program)<--(s:study)<--(a:study_arm)<--(co:cohort)
OPTIONAL MATCH (co)<--(case:case)<--(cycle:cycle),
      (diag:diagnosis)-->(case)<--(demo:demographic),
      (e:enrollment)-->(case)
RETURN p, s, a, co, case, cycle, diag, demo, e
//Studie BY Program
MATCH (s:study)
OPTIONAL MATCH (p:program)<-[*]-(s)
OPTIONAL MATCH (s)<-[*]-(c:case)
RETURN p.program_id AS program_id,
       s.clinical_study_id AS clinical_study_id,
       s.clinical_study_designation AS clinical_study_designation,
       s.clinical_study_name AS clinical_study_name,
       s.clinical_study_description AS clinical_study_description,
       s.clinical_study_type AS clinical_study_type,
       s.date_of_iacuc_approval AS date_of_iacuc_approval,
       s.dates_of_conduct AS dates_of_conduct,
       count(c) AS numberOfCases
ORDER BY clinical_study_designation

// CASE Overview Query
MATCH (s:study)
WITH collect(DISTINCT(s.clinical_study_designation)) AS all_studies
MATCH (d:demographic)
WITH collect(DISTINCT(d.breed)) AS all_breeds, collect(DISTINCT(d.sex)) AS all_sexes, all_studies
MATCH (d:diagnosis)
WITH collect(DISTINCT(d.disease_term)) AS all_diseases, all_breeds, all_sexes, all_studies
MATCH (p:program)<-[*]-(s:study)<-[*]-(c:case)<--(demo:demographic), (c)<--(diag:diagnosis)
  WHERE s.clinical_study_designation IN CASE $study_codes WHEN [] THEN all_studies
    ELSE $study_codes
    END
  AND demo.breed IN CASE $breeds WHEN [] THEN all_breeds
    ELSE $breeds
    END
  AND diag.disease_term IN CASE $diagnoses WHEN [] THEN all_diseases
    ELSE $diagnoses
    END
  AND demo.sex IN CASE $sexes WHEN [] THEN all_sexes
    ELSE $sexes
    END
OPTIONAL MATCH (f:file)-[*]->(c)
OPTIONAL MATCH (samp:sample)-[*]->(c)
WITH DISTINCT c AS c, p, s, demo, diag, f, samp
RETURN c.case_id AS case_id,
       s.clinical_study_designation AS study_code,
       p.program_acronym AS program,
       s.clinical_study_type AS study_type,
       demo.breed AS breed,
       diag.disease_term AS diagnosis,
       diag.stage_of_disease AS stage_of_disease,
       diag.primary_disease_site AS disease_site,
       demo.patient_age_at_enrollment AS age,
       demo.sex AS sex,
       demo.neutered_indicator AS neutered_status,
       collect(DISTINCT(f.file_type)) AS data_types,
       collect(DISTINCT(f.file_format)) AS file_formats,
       collect(DISTINCT(f)) AS files,
       collect(DISTINCT(samp.sample_id)) AS samples,
       collect(DISTINCT(samp)) AS sample_list

// casesInList query
MATCH (p:program)<-[*]-(s:study)<-[*]-(c:case)<--(demo:demographic), (c)<--(diag:diagnosis)
     WHERE c.case_id IN $case_ids
OPTIONAL MATCH (f:file)-[*]->(c)
OPTIONAL MATCH (samp:sample)-[*]->(c)
WITH DISTINCT c AS c, p, s, demo, diag, f, samp
RETURN c.case_id AS case_id,
       s.clinical_study_designation AS study_code,
       p.program_acronym AS program,
       s.clinical_study_type AS study_type,
       demo.breed AS breed,
       diag.disease_term AS diagnosis,
       diag.stage_of_disease AS stage_of_disease,
       diag.primary_disease_site AS disease_site,
       demo.patient_age_at_enrollment AS age,
       demo.sex AS sex,
       demo.neutered_indicator AS neutered_status,
       collect(DISTINCT(f.file_type)) AS data_types,
       collect(DISTINCT(f.file_format)) AS file_formats,
       collect(DISTINCT(f)) AS files,
       collect(DISTINCT(samp.sample_id)) AS samples

 // Get ALL files FOR SBG
MATCH (f:file)-[*]->(s:study)
OPTIONAL MATCH (f)-[*]->(c:case)
OPTIONAL MATCH (c)-->(co:cohort)-->(arm:study_arm)
OPTIONAL MATCH (demo:demographic)-->(c)
OPTIONAL MATCH (diag:diagnosis)-->(c)
OPTIONAL MATCH (f)-[*]->(v:visit)
OPTIONAL MATCH (f)-->(samp:sample)
WITH DISTINCT(f) AS f, s, c, co, arm, demo, diag, v, samp
RETURN s.clinical_study_designation AS clinical_study_designation, s.clinical_study_name AS clinical_study_name,
       arm.arm AS arm,
       co.cohort_description AS cohort_description, co.cohort_dose AS cohort_dose,
       c.case_id AS case_id,
       demo.breed AS breed, demo.weight AS weight, demo.sex AS sex, demo.neutered_indicator AS neutered_indicator, demo.patient_age_at_enrollment AS patient_age_at_enrollment,
       diag.disease_term AS disease_term, diag.stage_of_disease AS stage_of_disease, diag.primary_disease_site AS primary_disease_site, diag.concurrent_disease AS concurrent_disease, diag.concurrent_disease_type AS concurrent_disease_type,
       v.visit_date AS visit_date, v.visit_number AS visit_number, v.day_in_cycle AS day_in_cycle,
       samp.sample_id AS sample_id, samp.sample_type AS sample_type, samp.general_sample_pathology AS general_sample_pathology, samp.specific_sample_pathology AS specific_sample_pathology, samp.sample_site AS sample_site, samp.date_of_sample_collection AS date_of_sample_collection, samp.necropsy_sample AS necropsy_sample, samp.percentage_tumor AS percentage_tumor, samp.percentage_stroma AS percentage_stroma, samp.comment AS comment,
       f.file_name AS file_name, f.file_type AS file_type, f.file_description AS file_description, f.file_format AS file_format, f.file_size AS file_size, f.md5sum AS md5sum, f.file_locations AS file_locations, f.uuid AS uuid
ORDER BY file_type, case_id

// Get all files for SBG/IndexD
MATCH (f:file)
WITH collect(f.uuid) AS all_files
MATCH (f:file)-[*]->(s:study)
  WHERE f.uuid IN CASE $file_ids WHEN [] THEN all_files
    ELSE $file_ids
    END
OPTIONAL MATCH (f)-[*]->(c:case)
OPTIONAL MATCH (f)-[*]->(co:cohort)
OPTIONAL MATCH (f)-[*]->(arm:study_arm)
OPTIONAL MATCH (demo:demographic)-->(c)
OPTIONAL MATCH (diag:diagnosis)-->(c)
OPTIONAL MATCH (f)-[*]->(v:visit)
OPTIONAL MATCH (f)-->(samp:sample)
WITH DISTINCT (f) AS f, s, c, co, arm, demo, diag, v, samp
RETURN s.clinical_study_designation AS clinical_study_designation, s.clinical_study_name AS clinical_study_name,
       arm.arm AS arm,
       co.cohort_description AS cohort_description, co.cohort_dose AS cohort_dose,
       c.case_id AS case_id,
       demo.breed AS breed, demo.weight AS weight, demo.sex AS sex, demo.neutered_indicator AS neutered_indicator,
       demo.patient_age_at_enrollment AS patient_age_at_enrollment,
       diag.disease_term AS disease_term, diag.stage_of_disease AS stage_of_disease,
       diag.primary_disease_site AS primary_disease_site, diag.concurrent_disease AS concurrent_disease,
       diag.concurrent_disease_type AS concurrent_disease_type,
       v.visit_date AS visit_date, v.visit_number AS visit_number, v.day_in_cycle AS day_in_cycle,
       samp.sample_id AS sample_id, samp.sample_type AS sample_type,
       samp.general_sample_pathology AS general_sample_pathology,
       samp.specific_sample_pathology AS specific_sample_pathology, samp.sample_site AS sample_site,
       samp.date_of_sample_collection AS date_of_sample_collection, samp.necropsy_sample AS necropsy_sample,
       samp.percentage_tumor AS percentage_tumor, samp.percentage_stroma AS percentage_stroma, samp.comment AS comment,
       f.file_name AS file_name, f.file_type AS file_type, f.file_description AS file_description,
       f.file_format AS file_format, f.file_size AS file_size, f.md5sum AS md5sum,
       f.file_locations AS file_locations, f.uuid AS uuid
  ORDER BY file_type, case_id