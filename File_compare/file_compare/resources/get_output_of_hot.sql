select t.location || t.distributed_name as path
  from opr_report_log t
 where t.bsp_code = '$bsp'
   and trunc(t.create_date) = to_date('$date', 'yyyy-mm-dd')
   and t.output_group = 'HOT'
   and t.status = 'COMPLETED'
   and t.file_size > 0
   and instr(upper(t.distributed_name), '.TXT') = 0
   and t.distributed_name <> 'MIAGTXNASD'
