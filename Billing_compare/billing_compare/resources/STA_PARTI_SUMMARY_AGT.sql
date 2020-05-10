    select isis.code, isis2.code,isis.INT_DOM,isis2.INT_DOM,isis.billcateg_code,isis2.billcateg_code,              
           nvl(isis.bal,0)-nvl(isis2.bal,0) bal,                                                                   
           nvl(isis.cais,0)-nvl(isis2.cais,0) cais,                                                                
           nvl(isis.ccis,0)-nvl(isis2.ccis,0) ccis,                                                                
           nvl(isis.adm,0)-nvl(isis2.adm,0) adm,                                                                   
           nvl(isis.rfnd,0)-nvl(isis2.rfnd,0) refund,                                                              
           nvl(isis.acm,0)-nvl(isis2.acm,0) acm,                                                                   
           nvl(isis.comm,0)-nvl(isis2.comm,0) comm,                                                                
           nvl(isis.txca,0)-nvl(isis2.txca,0) txca,                                                                
           nvl(isis.txcc,0)-nvl(isis2.txcc,0) txcc,                                                                
           nvl(isis.toca,0)-nvl(isis2.toca,0) toca,                                                                
           nvl(isis.efcois,0)-nvl(isis2.efcois,0) effis                                                            
    from                                                                                                           
    ( select t.code,                                                                                               
           t.billcateg_code,                                                                                       
           t.INT_DOM,                                                                                              
           t.CURR_CODE,                                                                                            
           sum(t.isu_tax_ca_total + t.isu_ticket_ca_total) cais,                                                   
           sum(t.isu_tax_cc_total + t.isu_ticket_cc_total) ccis,                                                   
           sum(t.adm_ticket_ca_total + t.adm_tax_ca_total) adm,                                                    
           sum(t.refd_ticket_ca_total + t.refd_tax_ca_total) rfnd,                                                 
           sum(t.acm_ticket_ca_total + t.acm_tax_ca_total) acm,                                                    
           sum(t.bal_pay_total) bal,                                                                               
           sum(t.eff_cms_total) comm,                                                                              
           sum(t.isu_tax_ca_total) txca,                                                                           
           sum(t.isu_tax_cc_total) txcc,                                                                           
           sum(t.toca_total) toca,                                                                                 
           sum(t.isu_eff_cms_total) efcois                                                                         
      from '&nbs'.sta_parti_summary t                                                                               
      where                                                                                                        
      t.parti_type in (select P.PARTI_TYPE from ref_parti_typ p where p.process_type in ('AGENT','PSEUDO AGENT'))  
      and t.bsp_code = '&bsp' and t.bill_period = '&period'                                                                                          
      group by t.code,t.curr_code,t.billcateg_code,t.INT_DOM ) isis                                                
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
      a.parti_type in (select P.PARTI_TYPE from ref_parti_typ p where p.process_type in ('AGENT','PSEUDO AGENT'))  
      and a.bsp_code = '&bsp' and a.bill_period = '&period'                                                                                          
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
      NVL(isis.txca,0) != nvl(isis2.txca,0)                                                                        
    or                                                                                                             
      nvl(isis.txcc,0) != nvl(isis2.txcc,0)                                                                        
    or                                                                                                             
      NVL(isis.toca,0) != nvl(isis2.toca,0)                                                                        
    or                                                                                                             
      NVL(isis.efcois,0) != nvl(isis2.efcois,0)                                                                    
    order by isis.code                                                                                              

      
