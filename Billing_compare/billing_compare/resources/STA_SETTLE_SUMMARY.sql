
 select isis.air_code,isis2.air_code,isis.INT_DOM,isis2.INT_DOM,isis.billcateg_code,isis2.billcateg_code,  
           isis.cutp,isis2.cutp,                                                                              
           isis.bill_total,isis2.bill_total,                                                                  
           nvl(isis.bill_total,0)-nvl(isis2.bill_total,0) BDIFF,                                              
           isis.settle_total,isis2.settle_total,                                                              
           nvl(isis.settle_total,0)-nvl(isis2.settle_total,0) SDIFF                                           
    from                                                                                                      
    (select a.air_code,                                                                                       
            a.curr_code cutp,                                                                                 
            a.billcateg_code,                                                                                 
            a.INT_DOM,                                                                                        
            sum(nvl(a.billed_amount,0)) bill_total,                                                           
            sum(nvl(a.settl_total,0)) settle_total                                                            
     from '&nbs'.sta_settle_summary a                                                                          
     where a.bsp_code = '&bsp' and a.bill_period = '&period'                                              
     group by a.air_code,a.curr_code,a.billcateg_code,a.INT_DOM) isis2                                        
    FULL OUTER JOIN                                                                                           
    (                                                                                                         
    select  a.air_code,                                                                                       
            a.curr_code cutp,                                                                                 
            a.billcateg_code,                                                                                 
            a.INT_DOM,                                                                                        
            sum(nvl(a.billed_amount,0)) bill_total,                                                           
            sum(nvl(a.settl_total,0)) settle_total                                                            
     from '&nbs'2.sta_settle_summary a                                                                          
     where a.bsp_code = '&bsp' and a.bill_period = '&period'                                              
     group by a.air_code,a.curr_code,a.billcateg_code,a.INT_DOM) isis                                         
    on isis2.air_code = trim(isis.air_code)                                                                   
    and isis2.cutp = isis.cutp                                                                                
    and isis2.billcateg_code = isis.billcateg_code                                                            
    and isis2.INT_DOM = isis.INT_DOM                                                                          
    where nvl(isis.bill_total,0) != nvl(isis2.bill_total,0)                                                   
    or nvl(isis.settle_total,0) != nvl(isis2.settle_total,0)                                                  
    or isis.air_code is null                                                                                  
    or isis2.air_code is null                                                                                 
    order by isis.air_code                                                                                   
      
