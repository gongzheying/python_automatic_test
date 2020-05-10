delete from qrtz_job_listeners;
delete from qrtz_trigger_listeners;
delete from qrtz_fired_triggers;
delete from qrtz_simple_triggers;
delete from qrtz_cron_triggers;
delete from qrtz_blob_triggers;
delete from qrtz_triggers;
delete from qrtz_job_details;
delete from qrtz_calendars;
delete from qrtz_paused_trigger_grps;
delete from qrtz_scheduler_state;
update ref_inputchannel	t set t.next_scan_time =null;
update opr_job t set t.quartz_reseted = '1' where t.job_status = 'SCHEDULED';

