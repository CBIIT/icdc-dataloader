// SubjectOverview
MATCH (ss:study_subject)
OPTIONAL MATCH (ss)-[*]->(p:program)
OPTIONAL MATCH (ss)-[*]->(s:study)
OPTIONAL MATCH (ss)<-[*]-(sf:stratification_factor)
OPTIONAL MATCH (ss)<-[*]-(d:diagnosis)
OPTIONAL MATCH (ss)<-[*]-(tp:therapeutic_procedure)
OPTIONAL MATCH (ss)<-[*]-(demo:demographic_data)
OPTIONAL MATCH (ss)<-[*]-(f:file)
RETURN p.program_acronym AS program,
       s.study_acronym AS study_acronym,
       s.study_short_description AS study_short_description,
       ss.study_subject_id AS subject_id,
       ss.disease_subtype AS diagnosis,
       sf.grouped_recurrence_score AS recurrence_score,
       d.tumor_size_group AS tumor_size,
       d.tumor_grade AS tumor_grade,
       d.er_status AS er_status,
       d.pr_status AS pr_status,
       tp.chemotherapy_regimen AS chemotherapy,
       tp.endocrine_therapy_type AS endocrine_therapy,
       demo.menopause_status AS menopause_status,
       demo.age_at_index AS age_at_index,
       demo.survival_time AS survival_time,
       demo.survival_time_unit AS survival_time_unit,
       collect(f) AS files

// subjectsInList
MATCH (ss:study_subject)
  WHERE ss.study_subject_id IN $subject_ids
OPTIONAL MATCH (ss)-[*]->(p:program)
OPTIONAL MATCH (ss)-[*]->(s:study)
OPTIONAL MATCH (ss)<-[*]-(sf:stratification_factor)
OPTIONAL MATCH (ss)<-[*]-(d:diagnosis)
OPTIONAL MATCH (ss)<-[*]-(tp:therapeutic_procedure)
OPTIONAL MATCH (ss)<-[*]-(demo:demographic_data)
RETURN p.program_acronym AS program,
       s.study_acronym AS study_acronym,
       ss.study_subject_id AS subject_id,
       ss.disease_subtype AS diagnosis,
       sf.grouped_recurrence_score AS recurrence_score,
       d.tumor_size_group AS tumor_size,
       d.er_status AS er_status,
       d.pr_status AS pr_status,
       demo.age_at_index AS age_at_index,
       demo.survival_time AS survival_time,
       demo.survival_time_unit AS survival_time_unit

// filesOfSubjects
MATCH (ss:study_subject)<-[*]-(f:file), (parent)<--(f)
  WHERE ss.study_subject_id IN $subject_ids
RETURN ss.study_subject_id AS subject_id,
       f.file_name AS file_name,
       f.file_type AS file_type,
       head(labels(parent)) AS association,
       f.file_description AS file_description,
       f.file_format AS file_format,
       f.file_size AS file_size,
       f.file_id AS file_id,
       f.md5sum AS md5sum