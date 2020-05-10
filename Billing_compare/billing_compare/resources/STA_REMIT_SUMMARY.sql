    select isis.agent_code,isis2.agent_code,isis.INT_DOM,isis2.INT_DOM,isis.billcateg_code,isis2.billcateg_code,  
           isis.cutp,isis2.cutp,                                                                                  
           isis.bill_total,isis2.bill_total,                                                                      
           nvl(isis.bill_total,0)-nvl(isis2.bill_total,0) BDIFF,                                                  
           isis.remit_total,isis2.remit_total,                                                                    
           nvl(isis.remit_total,0)-nvl(isis2.remit_total,0) SDIFF                                                 
    from                                                                                                          
       (select a.agent_code,                                                                                      
               a.curr_code cutp,                                                                                  
               a.billcateg_code,                                                                                  
               a.INT_DOM,                                                                                         
               sum(nvl(a.billed_amount,0)) bill_total,                                                            
               sum(nvl(a.total,0)) remit_total                                                                    
          from '&nbs'.sta_remit_summary a                                                                          
          where a.bsp_code = '&bsp' and a.bill_period = '&period'                           
         group by a.agent_code,a.curr_code,a.billcateg_code,a.INT_DOM) isis2                                      
    FULL OUTER JOIN                                                                                               
    (                                                                                                             
    select a.agent_code,                                                                                          
               a.curr_code cutp,                                                                                  
               a.billcateg_code,                                                                                  
               a.INT_DOM,                                                                                         
               sum(nvl(a.billed_amount,0)) bill_total,                                                            
               sum(nvl(a.total,0)) remit_total                                                                    
          from '&nbs'2.sta_remit_summary a                                                                          
          where a.bsp_code = '&bsp' and a.bill_period = '&period'                           
         group by a.agent_code,a.curr_code,a.billcateg_code,a.INT_DOM ) isis                                      
    on isis2.agent_code = trim(isis.agent_code)                                                                   
       and isis2.cutp = isis.cutp                                                                                 
       and isis2.billcateg_code = isis.billcateg_code                                                             
       and isis2.INT_DOM = isis.INT_DOM                                                                           
    where nvl(isis.bill_total,0) != nvl(isis2.bill_total,0)                                                       
       or nvl(isis.remit_total,0) != nvl(isis2.remit_total,0)                                                     
       or isis2.agent_code is null                                                                                
       or isis.agent_code is null                                                                                 
       order by isis.agent_code                                                                                    
