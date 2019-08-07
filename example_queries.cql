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