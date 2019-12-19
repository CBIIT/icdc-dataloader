CREATE INDEX ON :program(program_acronym);
CREATE INDEX ON :study(clinical_study_designation);
CREATE INDEX ON :study_arm(arm);
CREATE INDEX ON :cohort(cohort_description);
CREATE INDEX ON :visit(visit_id);
CREATE INDEX ON :sample(sample_id);
CREATE INDEX ON :file(uuid);
CREATE INDEX ON :demographic(uuid);
CREATE INDEX ON :cycle(uuid);
CREATE INDEX ON :principal_investigator(uuid);
CREATE INDEX ON :diagnosis(diagnosis_id);
CREATE INDEX ON :diagnosis(uuid);
CREATE INDEX ON :enrollment(enrollment_id);
CREATE INDEX ON :enrollment(uuid);
CREATE INDEX ON :prior_surgery(uuid);
CREATE INDEX ON :physical_exam(uuid);
CREATE INDEX ON :vital_signs(uuid);
CREATE INDEX ON :disease_extent(uuid);


