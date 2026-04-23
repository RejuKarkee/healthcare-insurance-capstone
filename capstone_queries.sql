--UC1: Which disease has the maximum number of claims
CREATE TABLE project_output.uc1_max_claims_by_disease AS
SELECT d.disease_name, 
       COUNT(c.claim_id) AS total_claims
FROM project_input.claims c
JOIN project_input.disease d 
ON d.disease_id = c.disease_id
GROUP BY d.disease_name
ORDER BY total_claims DESC
LIMIT 1;

--UC2: Subscribers under age 30 who subscribe to any subgroup
CREATE TABLE project_output.uc2_subscribers_under_30 AS
SELECT s.sub_id, s.first_name, s.last_name, s.subgrp_id,
       DATEDIFF('year', s.birth_date::DATE, CURRENT_DATE) AS age
FROM project_input.subscriber s
WHERE DATEDIFF('year', s.birth_date::DATE, CURRENT_DATE) < 30
AND s.subgrp_id IS NOT NULL;

-- UC3: Which group has the maximum subgroups?
CREATE TABLE project_output.uc3_group_max_subgroups AS
SELECT gs.grp_id, g.grp_name,
       COUNT(gs.subgrp_id) AS total_subgroups
FROM project_input.group_subgroup gs
JOIN project_input.groups g ON g.grp_id = gs.grp_id
GROUP BY gs.grp_id, g.grp_name
ORDER BY total_subgroups DESC
LIMIT 1;

-- UC4: Which hospital serves the greatest number of patients?
CREATE TABLE project_output.uc4_hospital_max_patients AS
SELECT p.hospital_id, h.hospital_name,
       COUNT(p.patient_id) AS total_patients
FROM project_input.patient_records p
JOIN project_input.hospital h ON h.hospital_id = p.hospital_id
GROUP BY p.hospital_id, h.hospital_name
ORDER BY total_patients DESC
LIMIT 1;
 

-- UC5: Which subgroup has the greatest number of subscribers?
CREATE TABLE project_output.uc5_subgroup_max_subscribers AS
SELECT s.subgrp_id, sg.subgrp_name,
       COUNT(s.sub_id) AS total_subscribers
FROM project_input.subscriber s
JOIN project_input.subgroup sg ON sg.subgrp_id = s.subgrp_id
GROUP BY s.subgrp_id, sg.subgrp_name
ORDER BY total_subscribers DESC
LIMIT 1;
 

-- UC6: Total number of claims that were rejected
CREATE TABLE project_output.uc6_total_rejected_claims AS
SELECT COUNT(claim_id) AS total_rejected_claims
FROM project_input.claims
WHERE claim_or_rejected = 'N';
 

-- UC7: From which city do most claims come?
CREATE TABLE project_output.uc7_max_claims_by_city AS
SELECT p.city,
       COUNT(c.claim_id) AS total_claims
FROM project_input.claims c
JOIN project_input.patient_records p ON p.patient_id::VARCHAR = c.patient_id::VARCHAR
GROUP BY p.city
ORDER BY total_claims DESC
LIMIT 1;
 

-- UC8: Government or private policies mostly subscribed?
CREATE TABLE project_output.uc8_govt_vs_private AS
SELECT g.grp_type,
       COUNT(s.sub_id) AS total_subscribers
FROM project_input.subscriber s
JOIN project_input.group_subgroup gs ON gs.subgrp_id = s.subgrp_id
JOIN project_input.groups g ON g.grp_id = gs.grp_id
GROUP BY g.grp_type
ORDER BY total_subscribers DESC;
 

-- UC9: Average monthly premium subscribers pay
CREATE TABLE project_output.uc9_avg_monthly_premium AS
SELECT ROUND(AVG(sg.monthly_premium::DECIMAL), 2) AS avg_monthly_premium
FROM project_input.subscriber s
JOIN project_input.subgroup sg ON sg.subgrp_id = s.subgrp_id;
 

-- UC10: Which group is most profitable?
CREATE TABLE project_output.uc10_most_profitable_group AS
SELECT grp_id, grp_name, grp_type,
       premium_written::DECIMAL AS premium_written
FROM project_input.groups
ORDER BY premium_written::DECIMAL DESC
LIMIT 1;
 

-- UC11: Patients below age 18 admitted for cancer
CREATE TABLE project_output.uc11_patients_under_18_cancer AS
SELECT p.patient_id, p.patient_name, p.patient_gender,
       p.patient_birth_date,
       DATEDIFF('year', p.patient_birth_date::DATE, CURRENT_DATE) AS age,
       d.disease_name
FROM project_input.patient_records p
JOIN project_input.disease d ON d.disease_id = p.disease_id
WHERE DATEDIFF('year', p.patient_birth_date::DATE, CURRENT_DATE) < 18
AND LOWER(d.disease_name) LIKE '%cancer%';
 

-- UC12: Patients with cashless insurance and total charges >= 50,000
CREATE TABLE project_output.uc12_cashless_high_charges AS
SELECT c.claim_id, c.patient_id, c.claim_type,
       c.claim_amount, c.claim_or_rejected
FROM project_input.claims c
WHERE LOWER(c.claim_type) LIKE '%cashless%'
AND c.claim_amount::DECIMAL >= 50000;
 

-- UC13: Female patients over age 40 who had knee surgery in the past year
CREATE TABLE project_output.uc13_female_over40_knee_surgery AS
SELECT p.patient_id, p.patient_name, p.patient_gender,
       p.patient_birth_date,
       DATEDIFF('year', p.patient_birth_date::DATE, CURRENT_DATE) AS age,
       d.disease_name
FROM project_input.patient_records p
JOIN project_input.disease d ON d.disease_id = p.disease_id
WHERE p.patient_gender = 'Female'
AND DATEDIFF('year', p.patient_birth_date::DATE, CURRENT_DATE) > 40
AND LOWER(d.disease_name) LIKE '%knee%';



