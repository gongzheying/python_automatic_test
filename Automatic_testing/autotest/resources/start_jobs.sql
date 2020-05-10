update opr_job t
   set expected_starttime = to_date('$date 00:00:00', 'yyyy-mm-dd hh24:mi:ss'),
       quartz_reseted     = 1
 where bsp_code in ('$bsp')
   and job_status = 'SCHEDULED'
   and t.frequency_type != 'AD_HOC'
   and job_type <> 'PROCESS_NET_FARE'
   and to_char(trunc(EXPECTED_STARTTIME, 'dd'), 'yyyy-mm-dd') = '$date'
