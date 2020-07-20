// ICDC
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

// Bento
CREATE INDEX ON :aliquot(aliquot_id);
CREATE INDEX ON :analyte(analyte_id);
CREATE INDEX ON :cross_reference_database(cross_reference_database_id);
CREATE INDEX ON :demographic_data(demographic_data_id);
CREATE INDEX ON :diagnosis(diagnosis_id);
CREATE INDEX ON :exposure(exposure_node_id);
CREATE INDEX ON :family_medical_history(family_history_id);
CREATE INDEX ON :file(file_id);
CREATE INDEX ON :follow_up(follow_up_id);
CREATE INDEX ON :fraction(fraction_id);
CREATE INDEX ON :institution(institution_id);
CREATE INDEX ON :laboratory_procedure(laboratory_procedure_id);
CREATE INDEX ON :program(program_id);
CREATE INDEX ON :project(project_id);
CREATE INDEX ON :report(report_id);
CREATE INDEX ON :sample(sample_id);
CREATE INDEX ON :stratification_factor(stratification_factor_id);
CREATE INDEX ON :study(study_id);
CREATE INDEX ON :study_subject(study_subject_id);
CREATE INDEX ON :therapeutic_procedure(therapeutic_procedure_id);

