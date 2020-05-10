select t.location || t.distributed_name as path
  from opr_report_log t
 where t.bsp_code = '$bsp'
   and trunc(t.create_date) = to_date('$date', 'yyyy-mm-dd')
   and t.output_group = 'CSI'
   and t.status = 'COMPLETED'
   and t.file_size > 0
   and t.parti_code in
       (select distinct pca.pca_code
          from ref_pca pca
         where pca.pca_seq in
               (select f.pca_seq
                  from ref_pca_fmt f, ref_glbpca_fmt g
                 where f.glbpca_fmt_seq = g.glbpca_fmt_seq
                   and g.file_format = 'CSI'
                   and (F.EFFECTIVE_FROM <= TO_DATE('$date', 'yyyy-mm-dd') AND
                       (F.EFFECTIVE_TO >= TO_DATE('$date', 'yyyy-mm-dd') OR F.EFFECTIVE_TO IS NULL)))
           and pca.bsp_code = :bsp_code)
