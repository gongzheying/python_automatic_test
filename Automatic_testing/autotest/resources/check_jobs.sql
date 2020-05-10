select t.bsp_code,
       sum(nvl(t.completed, 0)) as completed,
       sum(nvl(t.failed, 0)) as failed,
       sum(nvl(t.numbers, 0)) as numbers,
       sum(nvl(t.numbers, 0)) - sum(nvl(t.completed, 0)) - sum(nvl(t.failed, 0)) as diff
  from (SELECT fl.bsp_code,
               decode(fl.job_status, 'COMPLETED', count(1)) as completed,
               decode(fl.job_status, 'FAILED', count(1)) as failed,
               count(1) as numbers
          FROM OPR_JOB fl
         where fl.bsp_code in ('$bsp')
           and fl.job_name != 'GL-ASBSCONT'
           and fl.frequency_type != 'AD_HOC'
           and fl.job_type != 'PROCESS_NET_FARE'
           and to_char(trunc(fl.EXPECTED_STARTTIME, 'dd'), 'yyyy-mm-dd') = '$date'
         group by fl.bsp_code, fl.job_status) t
 group by t.bsp_code
