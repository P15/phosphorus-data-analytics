with dist_and_sub_dists as (
    select id from distributors
    where id = 15
       or parent_distributor_id = 15
), sent_report_ids as (
    select distinct report_id from event_logs where event_type = 'report_marked_state_reported' -- Report.state_reported
), reports_to_work_with as (
    select distinct r.id as report_id,
                    r.patient_user_id,
                    r.kit_id
    from reports r
             left join locations on r.patient_user_id = locations.patient_user_id
    where r.cached_distributor_id in (select id from dist_and_sub_dists)
--     and r.id not in (select report_id from sent_report_ids)
      AND r.sent_date AT TIME ZONE 'UTC' AT TIME ZONE 'EDT' >= '{}'
      AND r.sent_date AT TIME ZONE 'UTC' AT TIME ZONE 'EDT' <= '{}'
      and locations.state = '{}'
      and r.status in ('approved', 'sent', 'sent_to_patient', 'consultation_pending')
      and r.report_type_id = 283
 ), first_provider_location as (
    select
           reports_to_work_with.report_id,
           c.id as clinic_id,
   case
        when c.main_location_id is null then (array_agg(pl.id))[1]
        else c.main_location_id
    end as location_id
    from reports_to_work_with
    join reports on reports_to_work_with.report_id = reports.id
    join clinics c on reports.provider_clinic_id = c.id
    left join locations pl on pl.clinic_id = c.id
    group by reports_to_work_with.report_id, c.id
), report_provider_clinic as (
    select
        reports.id as report_id,
        C.id as clinic_id,
        C.phone as provider_phone,
        concat(PL.ADDRESS_1,' ',PL.ADDRESS_2) as Address,
        PL.CITY,
        PL.STATE,
        PL.ZIP,
        C.name as clinic_name
    from reports_to_work_with
    join reports on reports_to_work_with.report_id = reports.id
    join first_provider_location on reports_to_work_with.report_id = first_provider_location.report_id
    join locations PL on first_provider_location.location_id = PL.id
    join clinics C on first_provider_location.clinic_id = C.id
), report_specimens as (
    select reports_to_work_with.report_id,
                    (array_agg(samples.id))[1] as sample_id
    from reports_to_work_with
    join samples on samples.kit_id = reports_to_work_with.kit_id
    join sample_data_sources on sample_id = samples.id
    where samples.kit_id in (select kit_id from reports_to_work_with)
    and sample_data_sources.status in ('qc_pending', 'qc_hold', 'qc_approved', 'vc_complete', 'further_review')
    group by report_id, samples.id
), output_results as (
    select
       
                 'Phosphorus Elements' AS "Sending Application",
                 NULL AS "Sending Application ID",
                 NOW() AS "Message Date and Time",
                 NULL AS "Receiving Application",
                 'Phosphorus Diagnostics LLC' AS "Lab Name",
                 case
                     when distributors.clia_number is  not null then distributors.clia_number
                     else '31D2123554'
                     end
                     AS "Lab CLIA",
                 '400 Plaza Drive Suite 401' AS "Lab Address",
                 'Secaucus' AS "Lab City",
                 'NJ' AS "Lab State",
                 '07094' AS "Lab Zip",
                 '855-746-7423' AS "Lab Phone",
                 R.REPORT_TYPE_ID AS "Test Code",
                 RT.NAME AS "Test Name",
                 CASE
                     when s.collection_type = 'Saliva' then '95425-5'
                     when s.collection_type = 'Swab' then '94533-7'
                     end
                     AS "LOINC Code",
                 CASE
                     when s.collection_type = 'Saliva' then 'SARS-CoV-2 (COVID-19) N gene [Presence] in Saliva (oral fluid) by NAA with probe detection'
                     when s.collection_type = 'Swab' then 'SARS-CoV-2 (COVID-19) N gene [Presence] in Respiratory specimen by NAA with probe detection'
                     end
                     AS "LOINC Description",
               
                 'Phosphorus COVID-19 RT-qPCR Test_Phosphorus Diagnostics LLC_EUA' AS "Test Instrument",
                 R.RESULT AS "Result",
                 'F' AS "Result Status Code (NYS)",
                 R.SENT_DATE AS "Result Date and Time",
                 PU.ID AS "Patient ID",
                 PU.FIRST_NAME AS "Patient First Name",
                 PU.LAST_NAME AS "Patient Last Name",
                 PU.BIRTH_DATE AS "Patient Date of Birth",
			case
				when PU.GENDER = 'male' then 'M'
				when PU.GENDER = 'female' then 'F'
				else 'U'
			end
			AS "Patient Sex",		 
		CASE
			when array_to_string(array_agg(ETH.NAME), ',', ' ') = ' ' then 'Unknown'
			else array_to_string(array_agg(ETH.NAME), ',', ' ')
		end
		AS "Patient Race",
                 'not hispanic or latino' AS "Patient Ethnicity",
                 CONCAT(L.ADDRESS_1,' ',L.ADDRESS_2) AS "Patient Street Address",
                 L.CITY AS "Patient City",
                 L.STATE AS "Patient State",
                 split_part(L.ZIP,'-',1) AS "Patient Zip",
                 NULL AS "Patient County",
                case
                    when L.COUNTRY = 'US' then 'United States of America'
                    when L.COUNTRY = 'USA' then 'United States of America'
                    else L.country
                end
                  AS "Patient Country",
                 CASE
					WHEN (PU.PHONE IS NULL AND PL.PROVIDER_PHONE IS NULL AND C.PHONE IS NULL) THEN '855-746-7423'
					WHEN (PU.PHONE IS NULL AND PL.PROVIDER_PHONE IS NULL) THEN C.PHONE
					WHEN PU.PHONE IS NULL THEN PL.PROVIDER_PHONE
					else pu.phone
		 		end
						AS "Patient Phone",
                 case 
			when (l.state = 'IL' and s.collection_type = 'Saliva') then concat(split_part(C.NAME, '/',1) , ' -- self collection')
			else split_part(C.NAME, '/',1) 
		 end
			AS "Ordering Facility",
                 CASE
			WHEN (pl.address is null or pl.address = ' ') then '400 Plaza Drive Suite 401'
			else pl.address
		 end
			AS "Ordering Facility Address",

		 CASE
			WHEN (pl.address is null or pl.address = ' ') then 'Secaucus'
			else pl.city
		 end
                	AS "Ordering Facility City",

		 CASE
			WHEN (pl.address is null or pl.address = ' ') then 'NJ'
			else pl.state
		 end
                	AS "Ordering Facility State",
		 CASE
			WHEN (pl.address is null or pl.address = ' ') then '07094'
			else split_part(pl.zip,'-',1)
		 end
                	AS "Ordering Facility Zip",
		 CASE
					WHEN (PL.PROVIDER_PHONE IS NULL AND C.PHONE IS NULL) THEN '855-746-7423'
					WHEN C.PHONE IS NULL THEN split_part(PL.PROVIDER_PHONE, ',' ,1)
					else split_part(C.phone, ',',1)
		 end
			as "Ordering Facility Phone",
                 PR.FIRST_NAME AS "Provider First Name",
                 PR.LAST_NAME AS "Provider Last Name",
                 PR.TITLE AS "Provider Name Suffix",
                 CASE
			WHEN (pl.address is null or pl.address = ' ') then '400 Plaza Drive Suite 401'
			else pl.address
		 end
			AS "Provider Address",

		 CASE
			WHEN (pl.address is null or pl.address = ' ') then 'Secaucus'
			else pl.city
		 end
                	AS "Provider City",

		 CASE
			WHEN (pl.address is null or pl.address = ' ') then 'NJ'
			else pl.state
		 end
                	AS "Provider State",
		 CASE
			WHEN (pl.address is null or pl.address = ' ') then '07094'
			else split_part(pl.zip,'-',1)
		 end
                	AS "Provider Zip",

                 CASE
					WHEN (PL.PROVIDER_PHONE IS NULL AND C.PHONE IS NULL) THEN '855-746-7423'
					WHEN PL.PROVIDER_PHONE IS NULL THEN split_part(C.PHONE, ',' ,1)
					else split_part(pl.provider_phone, ',',1)
		 end
			as "Provider Phone Number",
                 PR.NPI AS "Provider NPI",
                 S.BARCODE AS "Specimen ID / Accession Number",
                 S.COLLECTION_DATE AS "Collection Date and Time",
                 S.COLLECTION_TYPE AS "Specimen Type",
                 CASE
                     when s.collection_type = 'Saliva' then 'Pharyngeal structure'
                     when s.collection_type = 'Swab' then 'Nasopharyngeal structure'
                     end
                 AS "Specimen Source/Site",
                 NULL AS "Notes",
                 NULL AS "Previous Test Exists (Y/N)",
                 NULL AS "Healthcare Worker (Y/N)",
                 NULL AS "Symptom Onset Date",
                 NULL AS "Hospitalized (Y/N)",
                 NULL AS "Congregate Care Resident (Y/N)",
                 NULL AS "Pregnant (Y/N)",

		 date_part('year', age(PU.BIRTH_DATE))::int as "Patient Age",
		 case
			when r.result = 'Positive' then 'A'
			when r.result = 'Negative' then 'N'
			else ''
		 end
		as "Test flag",
		case
			when r.result = 'Positive' then '10828004'
			when r.result = 'Negative' then '260385009'
			else '419984006'
		 end
		as "Result SNOMED",
		'Negative' as "Reference Range",
		CONCAT(pr.first_name,' ',pr.last_name) AS "Provider Full Name",
		r.id as "Report ID",
		'XX' as "Patient Identifier Type",
                 CASE
                     when s.collection_type = 'Saliva' then '119342007'
                     when s.collection_type = 'Swab' then '258500001'
			else '119342007'
                     end
                 AS "Specimen Source/Site SNOMED",
		'SCT' as "SCT",

                 CASE
                     when s.collection_type = 'Saliva' then '54066008'
                     when s.collection_type = 'Swab' then '71836000'
			else '54066008'
                     end
                 AS "Specimen Source/Site SNOMED (Alternative)",
		concat(pr.last_name,', ',pr.first_name) as "Provider last,first",
		'LN' as "LN",
		'1' as "1",
		'!!' as "!!",
		'L' as "L",
		 'U' as "U",
		'UNK' as "UNK",
		 NULL AS "NULLCOLUMN",
		'ECLRS' AS "ECLRS"
		
    from reports_to_work_with
             join reports r on reports_to_work_with.report_id = r.id
             join distributors on r.cached_distributor_id = distributors.id
             JOIN PATIENT_USERS PU ON PU.ID = R.PATIENT_USER_ID
             JOIN REPORT_TYPES RT ON RT.ID = R.REPORT_TYPE_ID
             left JOIN ETHNICITIES_PATIENT_USERS EP ON EP.PATIENT_USER_ID = PU.ID
             left JOIN ETHNICITIES ETH ON ETH.ID = EP.ETHNICITY_ID
             left JOIN LOCATIONS L ON L.PATIENT_USER_ID = PU.ID
             left JOIN report_provider_clinic PL ON R.id = pl.report_id
             JOIN CLINICS C ON C.ID = R.clinic_id
             left JOIN PRACTITIONERS PR ON PU.practitioner_id = PR.id
             left JOIN report_specimens RS ON RS.report_id = R.ID
             join samples s on RS.sample_id = s.id
    where s.collection_type IN ('Saliva','Swab')
    group by
        R.RESULT,
        R.SENT_DATE,
        PU.ID,
        R.REPORT_TYPE_ID,
        S.BARCODE,
        PU.FIRST_NAME,
        PU.LAST_NAME,
        PU.BIRTH_DATE,
        PU.GENDER,
        L.ADDRESS_1,
        L.ADDRESS_2,
        L.CITY,
        L.STATE,
        L.ZIP,
        L.COUNTRY,
        PU.PHONE,
        C.PHONE,
        C.NAME,
        PL.CITY,
        PL.STATE,
        PL.ZIP,
        PL.ADDRESS,
        RT.NAME,
        PL.provider_phone,
        PR.PHONE,
        PR.FIRST_NAME,
        PR.LAST_NAME,
        PR.TITLE,
        PL.CITY,
        PL.STATE,
        PL.ZIP,
        PR.PHONE,
        PR.NPI,
        S.COLLECTION_DATE,
        S.COLLECTION_TYPE,
        S.BARCODE,
        distributors.clia_number,
        R.id
) SELECT * from output_results


