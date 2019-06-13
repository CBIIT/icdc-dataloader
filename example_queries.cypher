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
