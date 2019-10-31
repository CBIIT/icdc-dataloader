# Example 1
:params {study_name: "Preclinical Evaluation of Three Idenoisoquinoline Candidates in Tumor-Bearing Dogs"};
MATCH (s:study)<-[:member_of]-(a:study_arm)<-[:member_of]-(c:cohort)<-[:member_of]-(case:case)<-[:of_case]-(d:demographic), 
      (case)<-[:of_case]-(diag:diagnosis) 
WHERE s.clinical_study_name = $study_name 
RETURN case.patient_id as patient_id, 
       d.breed as breed, 
       d.patient_age_at_enrollment as patient_age_at_enrollment,
       d.sex as sex,
       diag.disease_term as disease_term, 
       diag.stage_of_disease as stage_of_disease

# Example 2
MATCH (case:case)<-[:of_case]-(d:demographic) 
RETURN d.breed as breed, count(case) as `count of cases` 
ORDER BY d.breed

# Example 3
MATCH (diag:diagnosis)-[:of_case]->(case:case) 
RETURN diag.disease_term as disease_term, count(case) as `count of cases`
ORDER BY diag.disease_term

# Example 4
:params {study_name: "Preclinical Evaluation of Three Idenoisoquinoline Candidates in Tumor-Bearing Dogs", breeds: ["Golden Retriever", "Labrador Retriever", "Beagle", "Mixed Breed"], disease_terms: ["Lymphoma", "Malignant lymphoma"] }
MATCH (s:study)<-[:member_of]-(a:study_arm)<-[:member_of]-(c:cohort)<-[:member_of]-(case:case)<-[:of_case]-(d:demographic),
      (case)<-[:of_case]-(diag:diagnosis) 
WHERE s.clinical_study_name = $study_name 
      and d.breed in $breeds
      and diag.disease_term in $disease_terms
RETURN case.patient_id as patient_id, 
       d.breed as breed,
       d.patient_age_at_enrollment as `patient_age_at_enrollment`,
       d.sex as sex, 
       diag.disease_term as disease_term, 
       diag.stage_of_disease as stage_of_disease


# Export 4 nodes
match (s:study)<-[*]-(c:case)<-[:of_case]-(e:enrollment),
      (c)<-[:of_case]-(diag:diagnosis),
      (c)<-[:of_case]-(demo:demographic)
return s.submitter_id as study_code, c.patient_first_name as patient_first_name, c.patient_id as patient_id, c.project_id as project_id,
      demo.breed as breed, demo.date_of_birth as date_of_birth, demo.patient_age_at_enrollment as patient_age_at_enrollment, demo.sex as sex,
      e.initials as initials, e.enrollment_document_number as enrollment_document_number, e.date_of_registration as date_of_registration, e.cohort_description as cohort_description, e.date_of_informed_consent as date_of_informed_consent,
      e.site_short_name as site_short_name, e.veterinary_medical_center as veterinary_medical_center, e.patient_subgroup as patient_subgroup,
      diag.histology_cytopathology as histology_cytopathology, diag.date_of_histology_confirmation as date_of_histology_confirmation, diag.date_of_diagnosis as date_of_diagnosis, diag.primary_disease_site as primary_disease_site,
      diag.stage_of_disease as stage_of_disease, diag.disease_term as disease_term, diag.histological_grade as histological_grade

# Summary View
match (p:program)<--(s:study)<--(a:study_arm)<--(co:cohort)
return p, s, a, co

# Detailed View
match (p:program)<--(s:study)<--(a:study_arm)<--(co:cohort)
optional match (co)<--(case:case)<--(cycle:cycle),
      (diag:diagnosis)-->(case)<--(demo:demographic),
      (e:enrollment)-->(case)
return p, s, a, co, case, cycle, diag, demo, e

# Studie by Program
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
       count(c) as numberOfCases
ORDER BY clinical_study_designation

# Case Overview Query
MATCH (s:study)
WITH COLLECT(DISTINCT(s.clinical_study_designation)) AS all_studies
MATCH (d:demographic)
WITH COLLECT(DISTINCT(d.breed)) AS all_breeds, COLLECT(DISTINCT(d.sex)) AS all_sexes, all_studies AS all_studies
MATCH (d:diagnosis)
WITH COLLECT(DISTINCT(d.disease_term)) AS all_diseases, all_breeds AS all_breeds, all_sexes AS all_sexes, all_studies AS all_studies
MATCH (s:study)<--(:study_arm)<--(:cohort)<--(c:case)<--(demo:demographic),
      (c)<--(diag:diagnosis)
WHERE s.clinical_study_designation IN
      CASE $study_codes
      WHEN [] THEN all_studies
      ELSE $study_codes
      END
  AND demo.breed IN
      CASE $breeds
      WHEN [] THEN all_breeds
      ELSE $breeds
      END
  AND diag.disease_term IN
      CASE $diagnoses
      WHEN [] THEN all_diseases
      ELSE $diagnoses
      END
  AND demo.sex IN
      CASE $sexes
      WHEN [] THEN all_sexes
      ELSE $sexes
      END
RETURN c.case_id AS case_id,
 s.clinical_study_designation AS study_code,
 s.clinical_study_type AS study_type,
 demo.breed AS breed,
 diag.disease_term AS diagnosis,
 diag.stage_of_disease AS stage_of_disease,
 demo.patient_age_at_enrollment AS age,
 demo.sex AS sex,
 demo.neutered_indicator AS neutered_status

 # Get all files for 7 bridges
MATCH (f:file)-[*]->(s:study)
OPTIONAL MATCH (f)-[*]->(c:case)
OPTIONAL MATCH (c)-->(co:cohort)-->(arm:study_arm)
OPTIONAL MATCH (demo:demographic)-->(c)
OPTIONAl MATCH (diag:diagnosis)-->(c)
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