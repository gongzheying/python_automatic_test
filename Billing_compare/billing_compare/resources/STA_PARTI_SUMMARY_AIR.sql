    select isis.code, isis2.code,isis.INT_DOM,isis2.INT_DOM,isis.billcateg_code,isis2.billcateg_code,                             
           nvl(isis.bal,0)-nvl(isis2.bal,0) bal,                                                                                  
           nvl(isis.cais,0)-nvl(isis2.cais,0) cais,                                                                               
           nvl(isis.ccis,0)-nvl(isis2.ccis,0) ccis,                                                                               
           nvl(isis.adm,0)-nvl(isis2.adm,0) adm,                                                                                  
           nvl(isis.rfnd,0)-nvl(isis2.rfnd,0) refund,                                                                             
           nvl(isis.acm,0)-nvl(isis2.acm,0) acm,                                                                                  
           nvl(isis.comm,0)-nvl(isis2.comm,0) comm,                                                                               
           nvl(isis.efcois,0)-nvl(isis2.efcois,0) effis                                                                           
    from                                                                                                                          
    ( select s.code code,                                                                                                         
           s.billcateg_code,                                                                                                      
           s.INT_DOM,                                                                                                             
           s.CURR_CODE,                                                                                                           
             sum(s.isu_tax_ca_total + s.isu_ticket_ca_total) cais,                                                                
             sum(s.isu_tax_cc_total + s.isu_ticket_cc_total) ccis,                                                                
             sum(s.adm_ticket_ca_total + s.adm_tax_ca_total) adm,                                                                 
             sum(s.refd_ticket_ca_total + s.refd_tax_ca_total) rfnd,                                                              
             sum(s.acm_ticket_ca_total + s.acm_tax_ca_total) acm,                                                                 
             sum(s.bal_pay_total) bal,                                                                                            
           sum(s.eff_cms_total) comm,                                                                                             
              sum(s.isu_tax_ca_total) txca,                                                                                       
             sum(s.isu_tax_cc_total) txcc,                                                                                        
             sum(s.toca_total) toca,                                                                                              
           sum(s.isu_eff_cms_total) efcois                                                                                        
      from '&nbs'.sta_parti_summary s                                                                                              
      where                                                                                                                       
      s.parti_type in (select P.PARTI_TYPE from nbs_common.ref_parti_typ p where p.process_type in ('AIRLINE','PSEUDO AIRLINE'))  
      and s.bill_period = '&period'                                                                                                
      and s.bsp_code = '&bsp'                                                                                                      
      group by s.code,s.curr_code,s.billcateg_code,s.INT_DOM ) isis                                                               
    FULL OUTER JOIN                                                                                                               
    ( select a.code,                                                                                                              
           a.billcateg_code,                                                                                                      
           a.INT_DOM,                                                                                                             
           a.CURR_CODE,                                                                                                           
           sum(a.isu_tax_ca_total + a.isu_ticket_ca_total) cais,                                                                  
           sum(a.isu_tax_cc_total + a.isu_ticket_cc_total) ccis,                                                                  
           sum(a.adm_ticket_ca_total + a.adm_tax_ca_total) adm,                                                                   
           sum(a.refd_ticket_ca_total + a.refd_tax_ca_total) rfnd,                                                                
           sum(a.acm_ticket_ca_total + a.acm_tax_ca_total) acm,                                                                   
           sum(a.bal_pay_total) bal,                                                                                              
           sum(a.eff_cms_total) comm,                                                                                             
           sum(a.isu_tax_ca_total) txca,                                                                                          
           sum(a.isu_tax_cc_total) txcc,                                                                                          
           sum(a.toca_total) toca,                                                                                                
           sum(a.isu_eff_cms_total) efcois                                                                                        
      from '&nbs'2.sta_parti_summary a                                                                                              
      where                                                                                                                       
      a.parti_type in (select P.PARTI_TYPE from nbs_common.ref_parti_typ p where p.process_type in ('AIRLINE','PSEUDO AIRLINE'))  
      and a.bsp_code = '&bsp'  and a.bill_period = '&period'                                                                           
      group by a.code,a.curr_code,a.billcateg_code,a.INT_DOM ) isis2                                                              
    on isis.code = isis2.code                                                                                                     
       and isis.curr_code=isis2.curr_code                                                                                         
       and isis.billcateg_code=isis2.billcateg_code                                                                               
       and isis.INT_DOM=isis2.INT_DOM                                                                                             
    where                                                                                                                         
      (isis.code is null or isis2.code is null)                                                                                   
    or                                                                                                                            
      NVL(isis.cais,0) != nvl(isis2.cais,0)                                                                                       
    or                                                                                                                            
      NVL(isis.ccis,0) != nvl(isis2.ccis,0)                                                                                       
    or                                                                                                                            
      NVL(isis.adm,0) != nvl(isis2.adm,0)                                                                                         
    or                                                                                                                            
      NVL(isis.rfnd,0) != nvl(isis2.rfnd,0)                                                                                       
    or                                                                                                                            
      NVL(isis.acm,0) != nvl(isis2.acm,0)                                                                                         
    or                                                                                                                            
      NVL(isis.bal,0) != nvl(isis2.bal,0)                                                                                         
    or                                                                                                                            
      NVL(isis.comm,0) != nvl(isis2.comm,0)                                                                                       
    or                                                                                                                            
      NVL(isis.toca,0) != nvl(isis2.toca,0)                                                                                       
    or                                                                                                                            
      NVL(isis.efcois,0) != nvl(isis2.efcois,0)                                                                                   
    order by isis.code                                                                                                             
