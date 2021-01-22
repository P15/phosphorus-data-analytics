with dist_and_sub_dists as (
    select id from distributors
    where id = 15
      or parent_distributor_id = 15
),
sent_report_ids as (
    select distinct report_id from event_logs where event_type = 'report_marked_state_reported' and created_at >= '2021-01-01'
), 
reports_to_mark as(
select distinct r.id, r.patient_user_id
from reports r
join samples s ON S.KIT_ID = R.KIT_ID
join sample_data_sources sds on sds.sample_id = s.id
left join locations l on l.patient_user_id = r.patient_user_id
where sent_date AT TIME ZONE 'UTC' AT TIME ZONE 'EST' >= '{}'
	and sent_date AT TIME ZONE 'UTC' AT TIME ZONE 'EST' <= '{}'
	and l.state = '{}'
	and report_type_id = 283
   	and r.cached_distributor_id in (select id from dist_and_sub_dists)
	and r.status in ('approved', 'sent', 'sent_to_patient', 'consultation_pending')
	and s.collection_type in ('Saliva','Swab')
	and sds.status in ('qc_pending', 'qc_hold', 'qc_approved', 'vc_complete', 'further_review')
	and r.id not in (select report_id from sent_report_ids)
), 
mark_reported as (
	INSERT INTO "event_logs" (patient_user_id,report_id, event_type, agent_type, user_id,  "domain", created_at)
	select patient_user_id, id, 'report_marked_state_reported', 'human', 16025, 'script', current_timestamp from reports_to_mark)
	
	select * from reports_to_mark