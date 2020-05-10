    select isis.air, isis2.air,                                                             
           isis.trnc,isis2.trnc,                                                            
           isis.rpsi,isis2.rpsi,                                                            
           isis.form,isis2.form,                                                            
           isis.stat,isis2.stat,                                                            
           isis.billcateg_code,isis2.billcateg_code,                                        
           isis.stat,isis2.stat,                                                            
           nvl(isis.scuiss,0),nvl(isis2.scuiss,0),                                          
           nvl(isis.scuiss,0)-nvl(isis2.scuiss,0) scudiff,                                  
           nvl(isis.stdiss,0),nvl(isis2.stdiss,0),                                          
           nvl(isis.stdiss,0)-nvl(isis2.stdiss,0) stddiff,                                  
           nvl(isis.canscu,0),nvl(isis2.canscu,0),                                          
           nvl(isis.canscu,0)-nvl(isis2.canscu,0) candiff,                                  
           nvl(isis.canstd,0),nvl(isis2.canstd,0),                                          
           nvl(isis.canstd,0)-nvl(isis2.canstd,0) cstdiff,                                  
           nvl(isis.cccf,0),nvl(isis2.cccf,0),                                              
           nvl(isis.cccf,0)-nvl(isis2.cccf,0) ccdiff                                        
    from                                                                                    
    (select t.air_code air,                                                                 
     t.trnc trnc,                                                                           
     t.rpsi rpsi,                                                                           
     t.form_code form,                                                                      
     t.int_dom stat,                                                                        
     t.billcateg_code,                                                                      
     sum(t.scu_issued_count) scuiss,                                                        
     sum(t.std_issued_count) stdiss,                                                        
     sum(t.scu_cancelled_count) canscu,                                                     
     sum(t.std_cancelled_count) canstd,                                                     
     sum(t.cccf_tc_count) cccf                                                              
     from '&nbs'.sta_air_statis t                                                            
     where t.bsp_code = '&bsp' and t.bill_period = '&period'                                   
     group by t.air_code,t.trnc,t.rpsi,t.form_code,t.int_dom,t.billcateg_code) isis         
    FULL OUTER JOIN (                                                                       
    select  a.air_code air,                                                                 
            a.trnc trnc,                                                                    
            a.rpsi rpsi,                                                                    
            a.form_code form,                                                               
            a.int_dom stat,                                                                 
            a.billcateg_code,                                                               
            sum(a.scu_issued_count) scuiss,                                                 
            sum(a.std_issued_count) stdiss,                                                 
            sum(a.scu_cancelled_count) canscu,                                              
            sum(a.std_cancelled_count) canstd,                                              
            sum(a.cccf_tc_count) cccf                                                       
       from '&nbs'2.sta_air_statis a                                                          
           where a.bsp_code = '&bsp' and a.bill_period = '&period'                             
           group by a.air_code,a.trnc,a.rpsi,a.form_code,a.int_dom,a.billcateg_code) isis2  
     on trim(isis.air) = isis2.air                                                          
        and isis.trnc = isis2.trnc                                                          
        and isis.rpsi = isis2.rpsi                                                          
        and isis.form = isis2.form                                                          
        and isis.stat = isis2.stat                                                          
        and isis.billcateg_code = isis2.billcateg_code                                      
    where                                                                                   
     (isis2.air is null or isis.air is null)                                                
     or                                                              
     (isis2.air is not null and isis.scuiss <> isis2.scuiss)                                
     or                                                                                     
     (isis2.air is not null and isis.stdiss <> isis2.stdiss)                                
     or                                                                                     
     (isis2.air is not null and isis.canscu <> isis2.canscu)                                
     or                                                                                     
     (isis2.air is not null and isis.canstd <> isis2.canstd)                                
     or                                                                                     
     (isis2.air is not null and isis.cccf <> isis2.cccf)                                    
    order by isis.air,isis.trnc,isis.rpsi,isis.form,isis.stat,isis.billcateg_code           
                                                                                           
