MATCH (s:study)
OPTIONAL MATCH (s)<-[*]-(c:case)
WITH s, COUNT(DISTINCT c) AS cases
OPTIONAL MATCH (s)<-[*]-(samp:sample)
WITH s, cases, COUNT(DISTINCT samp) AS samples
OPTIONAL MATCH (s)<-[*]-(f:file)
WITH DISTINCT f, s, cases, samples
RETURN  s.clinical_study_designation AS study_code,
        COUNT(f) AS files, 
        SUM(f.file_size) AS totoal_size,
        cases,
        samples
