select t.bsp_code,
       sum(nvl(t.completed, 0)) as completed,
       sum(nvl(t.rejectd, 0)) as rejectd,
       sum(nvl(t.failed, 0)) as failed,
       sum(nvl(t.Numbers, 0)) as Numbers,
       sum(nvl(t.UNLOADED, 0)) as UNLOADED,
       sum(nvl(t.Numbers, 0)) - sum(nvl(t.completed, 0)) - sum(nvl(t.rejectd, 0)) - sum(nvl(t.failed, 0)) - sum(nvl(t.UNLOADED, 0)) as diff
  from (select nvl(fl.bsp_code, 'ACLI') bsp_code,
               decode(fl.file_status, 'COMPLETE', count(1)) as completed,
               decode(fl.file_status, 'REJECTED', count(1)) as rejectd,
               decode(fl.file_status, 'FAILED', count(1)) as failed,
               decode(fl.file_status, 'UNLOADED', count(1)) as UNLOADED,
               count(1) as Numbers
          from OPR_INPUTFILE_LOG fl
         where fl.bsp_code in ('$bsp')
           and to_char(trunc(fl.start_datetime,'dd'), 'yyyy-mm-dd') = '$date'
         group by fl.bsp_code, fl.file_status) t
 group by t.bsp_code
