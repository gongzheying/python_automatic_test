CREATE OR REPLACE package body PCK_AUTOTEST_BALANCE is

  PROCEDURE p_create_balance(bsp_code     in varchar2,
                             processdate  in varchar2) is
    vc_state   varchar(2000);
    vc_sysdate varchar(2000);
    vc_tablename varchar(2000);
    vc_exect   varchar(2000);
    vc_sql1    varchar(2000);
    vc_sql2    varchar(2000);
    vc_sql3    varchar(2000);

  BEGIN

    if (processdate is not null) then
      vc_sysdate := '_' || processdate;
    else
      vc_sysdate := '';
    end if;

    select count(*)
      into vc_exect
      from all_tables t
     where t.table_name =
           upper('RET_BALANCE_' || bsp_code || '' || vc_sysdate || '');

    select upper('RET_BALANCE_' || bsp_code || '' || vc_sysdate || '')
      into vc_tablename
      from dual;

    if (vc_exect = 1 and processdate is not null) then
      vc_sql1  := 'truncate table ' || vc_tablename || '';
      vc_state := 'truncate RET_BALANCE_' || bsp_code || '' ||
                  vc_sysdate || '';

      EXECUTE IMMEDIATE vc_sql1;
      COMMIT;

    elsif (vc_exect = 1) then
      vc_state := 'nothing';

    else
      vc_sql1 := 'create table RET_BALANCE_' || bsp_code || '' || vc_sysdate || ' ( ' ||
                 'PROCESSED_DATE VARCHAR2(200), FILE_NAME VARCHAR2(50), PROVIDER VARCHAR2(50),'||
                 'AIR VARCHAR2(50),  AGT VARCHAR2(50), ' ||
                 'DAIS VARCHAR2(50), DOC_NUM VARCHAR2(50), '||
                 'TRNC VARCHAR2(50), NRID VARCHAR2(10), ' ||
                 'TOUR VARCHAR2(50), IBSPS_AEBA VARCHAR2(50), '||
                 'IBSP_APBC VARCHAR2(50), TDAM VARCHAR2(50), ' ||
                 'TOTAL_TAX VARCHAR2(50), COTP1 VARCHAR2(50), '||
                 'CORT1 VARCHAR2(50), COAM1 VARCHAR2(50), ' ||
                 'CORT2 VARCHAR2(50), COAM2 VARCHAR2(50),'||
                 'COTP2 VARCHAR2(50), CC_PAYMENT_TOTAL VARCHAR2(50), ' ||
                 'CA_PAYMENT_TOTAL VARCHAR2(50), IBSP_MARKUP VARCHAR2(50), IBSPS_MARKUP VARCHAR2(50), ' ||
                 'IBSP_BAL VARCHAR2(50), IBSPS_BAL VARCHAR2(50),'||
                 'IBSP_CMFA VARCHAR2(50), IBSPS_CMFA VARCHAR2(50), ' ||
                 'IBSP_TAX_CREDIT VARCHAR2(50), IBSPS_TAX_CREDIT VARCHAR2(50), '||
                 'IBSP_CCIS VARCHAR2(50), IBSPS_TICKET_CREDIT VARCHAR2(50), ' ||
                 'IBSP_TAX_CA VARCHAR2(50), IBSPS_TAX_CA VARCHAR2(50), '||
                 'IBSP_TICKET_CASH VARCHAR2(50), IBSPS_TICKET_CASH VARCHAR2(50), ';
      vc_sql2 := 'IBSP_EFCO_A VARCHAR2(50), IBSPS_EFCO_A VARCHAR2(50), '||
                 'IBSP_EFCO_R VARCHAR2(50), IBSPS_EFCO_R VARCHAR2(50), IBSP_STDCM_A VARCHAR2(50), ' ||
                 'IBSPS_STD_CMS_A VARCHAR2(50), IBSP_STDCM_R VARCHAR2(50), '||
                 'IBSPS_STDCM_R VARCHAR2(50), IBSP_SPAM_A VARCHAR2(50), IBSPS_SPAM_A VARCHAR2(50), ' ||
                 'IBSP_SPAM_R VARCHAR2(50), IBSPS_SPAM_R VARCHAR2(50), '||
                 'IBSP_CMTX VARCHAR2(50), IBSPS_CMTX VARCHAR2(50), IBSP_TOCA_TOTAL VARCHAR2(50), ' ||
                 'IBSPS_TOCA_TOTAL VARCHAR2(50), IBSP_AIR_CC_PYMT VARCHAR2(50), '||
                 'IBSPS_AIR_CC_PYMT VARCHAR2(50), IBSP_NTFA VARCHAR2(50), ' ||
                 'IBSPS_NTFA VARCHAR2(50), IBSP_ESAC VARCHAR2(50), IBSPS_ESAC VARCHAR2(50), '||
                 'IBSP_COTP VARCHAR2(50), IBSPS_COTP1 VARCHAR2(50), ' ||
                 'IBSP_NR_METHOD VARCHAR2(50), IBSPS_NR_METHOD VARCHAR2(50), '||
                 'IBSP_DEL_F VARCHAR2(50), IBSPS_REJECT_FLAG VARCHAR2(50), ' ||
                 'IBSP_CARF VARCHAR2(50), IBSPS_CARF VARCHAR2(50), IBSP_SPTP VARCHAR2(50), '||
                 'IBSPS_SPTP VARCHAR2(50), IBSP_STAT VARCHAR2(50), ';
      vc_sql3 := 'IBSPS_INT_DOM VARCHAR2(50), IBSP_SALES_NUMBER VARCHAR2(200), Trans_seq VARCHAR2(200), ' ||
                 'DHOT_DATE VARCHAR2(20), REASON VARCHAR2(200) ) tablespace NBSTBL' ;

      vc_state := 'create AUTOTEST_BALANCE_' || bsp_code || '' || vc_sysdate || '' || ' completed!' ;

      EXECUTE IMMEDIATE vc_sql1 || vc_sql2 || vc_sql3;
      COMMIT;

    end if;

    dbms_output.put_line(vc_state);

  END;

 PROCEDURE p_insert_balance(bsp_code          in varchar2,
                            processdate       in varchar2) is
    i_vc_sysdate_sql      varchar(1000);
    vc_sysdate            varchar(2000);
    v_sql                 varchar(1000);
    vc_sql1               varchar(2000);
    vc_sql2               varchar(4000);
    vc_sql3               varchar(4000);
    vc_sql4               varchar(4000);
    vc_sql5               varchar(4000);
    vc_sql6               varchar(4000);
    vc_sql7               varchar(4000);
    vc_sql8               varchar(4000);
    vc_sql9               varchar(4000);
  BEGIN


   if (processdate is not null) then
      i_vc_sysdate_sql := 'to_char(n_inp.start_datetime, ''yyyymmdd'') = ' || '''' || processdate || '''' ;
      vc_sysdate       := '_' || processdate;
    else
      i_vc_sysdate_sql := '';
      vc_sysdate       := '';
    end if;

    vc_sql1 := 'insert into RET_BALANCE_' || bsp_code || '' || vc_sysdate ||
               ' ( ' || 'select distinct ' ||
               'TO_CHAR(n_inp.Start_Datetime,''YYYYMMDD''),' ||
               'n_INP.Orig_Filename, ' || 'n_ticket.sysprovider_code, ' ||
               'n_ticket.air_code, ' || 'n_ticket.agent_code, ' ||
               'n_ticket.dais dais, ' || 'n_ticket.Doc_Num DOC_NUM, ' ||
               'n_ticket.trnc TRNC, ' || 'n_ticket.NRID NRID, ' ||
               'n_ticket.tour_code, ' || 'n_ticket.aeba, ' ||
               'n_ticket.apbc, ' || 'n_ticket.tdam, ' ||
               'n_ticket.tax_total, ' || 'n_ticket.cotp1, ' ||
               'n_ticket.cort1, ' || 'n_ticket.coam1, ' ||
               'n_ticket.cort2, ' || 'n_ticket.coam2, ' ||
               'n_ticket.cotp2, ' || 'n_ticket.actual_cc_total, ' ||
               'n_ticket.actual_ca_total+n_ticket.pseudo_cash, ' ||
               'decode(NVL(o_ticket.AGT_MARKUP, 0), NVL(n_ticket.AGT_MARKUP, 0), '''', NVL(o_ticket.AGT_MARKUP, 0)) IBSP_MARKUP, ' ||
               'decode(NVL(n_ticket.AGT_MARKUP, 0), NVL(o_ticket.AGT_MARKUP, 0), '''', NVL(n_ticket.AGT_MARKUP, 0)) IBSPS_MARKUP, ';

    vc_sql2 := 'decode(NVL(o_ticket.bal_pay, 0), NVL(n_ticket.bal_pay, 0), '''', NVL(o_ticket.bal_pay, 0)) IBSP_BAL, ' ||
               'decode(NVL(n_ticket.bal_pay, 0), NVL(o_ticket.bal_pay, 0), '''', NVL(n_ticket.bal_pay, 0)) IBSPS_BAL, ' ||
               'decode(NVL(o_ticket.CMFA, 0), NVL(n_ticket.CMFA, 0), '''', NVL(o_ticket.CMFA, 0)) IBSP_CMFA, ' ||
               'decode(NVL(n_ticket.CMFA, 0), NVL(o_ticket.CMFA, 0), '''', NVL(n_ticket.CMFA, 0)) IBSPS_CMFA, ' ||
               'decode(NVL(o_ticket.TAX_CREDIT, 0), NVL(n_ticket.TAX_CREDIT, 0), '''', NVL(o_ticket.TAX_CREDIT, 0)) IBSP_TXCC, ' ||
               'decode(NVL(n_ticket.TAX_CREDIT, 0), NVL(o_ticket.TAX_CREDIT, 0), '''', NVL(n_ticket.TAX_CREDIT, 0)) IBSPS_TAX_CREDIT, ' ||
               'decode(NVL(o_ticket.TICKET_CREDIT, 0), NVL(n_ticket.TICKET_CREDIT, 0), '''', NVL(o_ticket.TICKET_CREDIT, 0)) IBSP_CCIS, ' ||
               'decode(NVL(n_ticket.TICKET_CREDIT, 0), NVL(o_ticket.TICKET_CREDIT, 0), '''', NVL(n_ticket.TICKET_CREDIT, 0)) IBSPS_TICKET_CREDIT, ' ||
               'decode(NVL(o_ticket.TAX_CA, 0), NVL(n_ticket.TAX_CA, 0), '''', NVL(o_ticket.TAX_CA, 0)) IBSP_TAX_CA, ';

    vc_sql3 := 'decode(NVL(n_ticket.TAX_CA, 0), NVL(o_ticket.TAX_CA, 0), '''', NVL(n_ticket.TAX_CA, 0)) IBSPS_TAX_CA, ' ||
               'decode(NVL(o_ticket.TICKET_CA, 0), NVL(n_ticket.TICKET_CA, 0), '''', NVL(o_ticket.TICKET_CA, 0)) IBSP_CAIS, ' ||
               'decode(NVL(n_ticket.TICKET_CA, 0), NVL(o_ticket.TICKET_CA, 0), '''', NVL(n_ticket.TICKET_CA, 0)) IBSPS_TICKET_CA, ' ||
               'decode(NVL(o_ticket.EFF_CMS_A, 0), NVL(n_ticket.EFF_CMS_A, 0), '''', NVL(o_ticket.EFF_CMS_A, 0)) IBSP_CMS_A, ' ||
               'decode(NVL(n_ticket.EFF_CMS_A, 0), NVL(o_ticket.EFF_CMS_A, 0), '''', NVL(n_ticket.EFF_CMS_A, 0)) IBSPS_EFF_CMS_A, ' ||
               'decode(NVL(o_ticket.EFF_CMS_R, 0), NVL(n_ticket.EFF_CMS_R, 0), '''', NVL(o_ticket.EFF_CMS_R, 0)) IBSP_CMS_R, ' ||
               'decode(NVL(n_ticket.EFF_CMS_R, 0), NVL(o_ticket.EFF_CMS_R, 0), '''', NVL(n_ticket.EFF_CMS_R, 0)) IBSPS_EFF_CMS_R, ' ||
               'decode(nvl(o_ticket.STD_CMS_A, 0), nvl(n_ticket.STD_CMS_A, 0), '''', nvl(o_ticket.STD_CMS_A, 0)) IBSP_STDCMS, ' ||
               'decode(nvl(n_ticket.STD_CMS_A, 0), nvl(o_ticket.STD_CMS_A, 0), '''', nvl(n_ticket.STD_CMS_A, 0)) IBSPS_STD_CMS_A, ';

    vc_sql4 := 'decode(nvl(o_ticket.STD_CMS_R, 0), nvl(n_ticket.STD_CMS_R, 0), '''', nvl(o_ticket.STD_CMS_R, 0)) IBSP_STDCMR, ' ||
               'decode(nvl(n_ticket.STD_CMS_R, 0), nvl(o_ticket.STD_CMS_R, 0), '''', nvl(n_ticket.STD_CMS_R, 0)) IBSPS_STD_CMS_R, ' ||
               'decode(nvl(o_ticket.SUP_CMS_A, 0), nvl(n_ticket.SUP_CMS_A, 0), '''', nvl(o_ticket.SUP_CMS_A, 0)) IBSP_SPAM, ' ||
               'decode(nvl(n_ticket.SUP_CMS_A, 0), nvl(o_ticket.SUP_CMS_A, 0), '''', nvl(n_ticket.SUP_CMS_A, 0)) IBSPS_SUP_CMS_A, ' ||
               'decode(nvl(o_ticket.SUP_CMS_R, 0), nvl(n_ticket.SUP_CMS_R, 0), '''', nvl(o_ticket.SUP_CMS_R, 0)) IBSP_SPRT, ' ||
               'decode(nvl(n_ticket.SUP_CMS_R, 0), nvl(o_ticket.SUP_CMS_R, 0), '''', nvl(n_ticket.SUP_CMS_R, 0)) IBSPS_SUP_CMS_R, ' ||
               'decode(nvl(o_ticket.comm_tax, 0), nvl(n_ticket.comm_tax, 0), '''', nvl(o_ticket.comm_tax, 0)) IBSP_txcm, ' ||
               'decode(nvl(n_ticket.comm_tax, 0), nvl(o_ticket.comm_tax, 0), '''', nvl(n_ticket.comm_tax, 0)) IBSPS_txcm, ' ||
               'decode(nvl(o_ticket.TOCA_TOTAL, 0), nvl(n_ticket.TOCA_TOTAL, 0), '''', nvl(o_ticket.TOCA_TOTAL, 0)) IBSP_TOCA, ';

    vc_sql5 := 'decode(nvl(n_ticket.TOCA_TOTAL, 0), nvl(o_ticket.TOCA_TOTAL, 0), '''', nvl(n_ticket.TOCA_TOTAL, 0)) IBSPS_TOCA_TOTAL, ' ||
               'decode(NVL(o_ticket.AIR_CC_PYMT, 0),NVL(n_ticket.AIR_CC_PYMT, 0), '''', NVL(o_ticket.AIR_CC_PYMT, 0)) IBSP_AIR_CC_PYMT, ' ||
               'decode(NVL(n_ticket.AIR_CC_PYMT, 0),NVL(o_ticket.AIR_CC_PYMT, 0), '''', NVL(n_ticket.AIR_CC_PYMT, 0)) IBSPS_AIR_CC_PYMT, ' ||
               'decode(nvl(o_ticket.NET_FARE, 0), nvl(n_ticket.NET_FARE, 0), '''', nvl(o_ticket.NET_FARE, 0)) IBSP_NTFA, ' ||
               'decode(nvl(n_ticket.NET_FARE, 0), nvl(o_ticket.NET_FARE, 0), '''', nvl(n_ticket.NET_FARE, 0)) IBSPS_NET_FARE, ' ||
               'decode(trim(o_ticket.ESAC), trim(n_ticket.ESAC), '''', trim(o_ticket.ESAC)) IBSP_ESAC, ' ||
               'decode(trim(n_ticket.ESAC), trim(o_ticket.ESAC), '''', trim(n_ticket.ESAC)) IBSPS_ESAC, ';

    vc_sql6 := 'decode(trim(o_ticket.COTP1), trim(n_ticket.COTP1), '''', trim(o_ticket.COTP1)) IBSP_COTP, ' ||
               'decode(trim(n_ticket.COTP1), trim(o_ticket.COTP1), '''', trim(n_ticket.COTP1)) IBSPS_COTP1, ' ||
               'decode(trim(o_ticket.NR_METHOD), trim(n_ticket.NR_METHOD), '''', trim(o_ticket.NR_METHOD)) IBSP_NRM_IN_COMMENT, ' ||
               'decode(trim(n_ticket.NR_METHOD), trim(o_ticket.NR_METHOD), '''', trim(n_ticket.NR_METHOD)) IBSPS_NE_METHOD, ' ||
               'DECODE(o_ticket.REJECT_FLAG, ''Y'', ''Y'', '''') IBSP_DEL_F, ' ||
               'decode(n_ticket.REJECT_FLAG, ''Y'', ''Y'', '''') IBSPS_REJECT_FLAG, ' ||
               'decode(trim(o_ticket.CARF), trim(n_ticket.CARF), '''', trim(o_ticket.CARF)) IBSP_CARF, ' ||
               'decode(trim(n_ticket.CARF), trim(o_ticket.CARF), '''', trim(n_ticket.CARF)) IBSPS_CARF, ';

    vc_sql7 := 'decode(o_ticket.SPTP, n_ticket.SPTP, '''', o_ticket.SPTP) IBSP_SPTP, ' ||
               'decode(n_ticket.SPTP, o_ticket.SPTP, '''', n_ticket.SPTP) IBSPS_SPTP, ' ||
               'decode(o_ticket.INT_DOM, n_ticket.INT_DOM, '''', o_ticket.INT_DOM) IBSP_STAT, ' ||
               'decode(n_ticket.INT_DOM, o_ticket.INT_DOM, '''', n_ticket.INT_DOM) IBSPS_INT_DOM, o_ticket.trans_seq,n_ticket.trans_seq,to_char(o_ticket.DAILY_HOT_DATE,''yyyymmdd''), ''''' ||
               'from nbs.bsd_ticket n_ticket, nbs2.bsd_ticket o_ticket,' ||
               'nbs_common.opr_inputfile_log n_inp, ' || 'nbs_common2.opr_inputfile_log o_inp ' ;

    vc_sql8 := 'where n_ticket.inputfile_log_seq = n_inp.inputfile_log_seq ' ||
               ' and n_ticket.bsp_code=' || '''' || bsp_code || '''' ||
               ' and o_ticket.inputfile_log_seq = o_inp.inputfile_log_seq '||
               ' and o_ticket.bsp_code=' || '''' || bsp_code || '''' ||
               ' and o_inp.orig_filename=n_inp.orig_filename' ||
               ' and o_inp.file_status = ''COMPLETE'' ' ||
               ' and n_inp.file_status = ''COMPLETE'' ' ||
               ' and o_inp.file_type = ''RET'' ' ||
               ' and n_inp.file_type = ''RET'' ' ||
               ' and o_inp.bill_period = n_inp.bill_period '||
               ' and o_ticket.trnn = n_ticket.trnn ' ||
               ' and o_ticket.doc_num = n_ticket.doc_num ' ||
               ' and o_ticket.trnc=n_ticket.trnc ' ||
               ' and o_ticket.sequence = n_ticket.sequence ' ||
               ' and ' || i_vc_sysdate_sql || '' ||
               ' and (( ' ||
               ' nvl(o_ticket.AGT_MARKUP, 0) != nvl(n_ticket.AGT_MARKUP, 0) or ' ||
               ' nvl(o_ticket.bal_pay, 0) != nvl(n_ticket.bal_pay, 0) or ' ||
               ' nvl(o_ticket.TAX_CA, 0) != nvl(n_ticket.TAX_CA, 0) or ' ||
               ' nvl(o_ticket.TICKET_CA, 0) != nvl(n_ticket.TICKET_CA, 0) or ' ||
               ' nvl(o_ticket.AIR_CC_PYMT, 0) != nvl(n_ticket.AIR_CC_PYMT, 0) or ' ||
               ' nvl(o_ticket.CMFA, 0) != nvl(n_ticket.CMFA, 0) or ';

    vc_sql9 := ' nvl(o_ticket.TAX_CREDIT, 0) != nvl(n_ticket.TAX_CREDIT, 0) or ' ||
               ' nvl(o_ticket.TICKET_CREDIT, 0) != nvl(n_ticket.TICKET_CREDIT, 0) or ' ||
               ' nvl(o_ticket.EFF_CMS_A, 0) != nvl(n_ticket.EFF_CMS_A, 0) or ' ||
               ' nvl(o_ticket.EFF_CMS_R, 0) != nvl(n_ticket.EFF_CMS_R, 0) or ' ||
               ' nvl(o_ticket.NET_FARE, 0) != nvl(n_ticket.NET_FARE, 0) or ' ||
               ' nvl(o_ticket.STD_CMS_A, 0) != nvl(n_ticket.STD_CMS_A, 0) or ' ||
               ' nvl(o_ticket.STD_CMS_R, 0) != nvl(n_ticket.STD_CMS_R, 0) or ' ||
               ' nvl(o_ticket.SUP_CMS_A, 0) != nvl(n_ticket.SUP_CMS_A, 0) or ' ||
               ' nvl(o_ticket.SUP_CMS_R, 0) != nvl(n_ticket.SUP_CMS_R, 0)  or ' ||
               ' nvl(o_ticket.NR_METHOD, 0) != NVL(n_ticket.NR_METHOD, 0) ' ||
               ' ) OR o_ticket.Reject_Flag != n_ticket.Reject_Flag))';

    dbms_output.put_line(vc_sql1);
    dbms_output.put_line(vc_sql2);
    dbms_output.put_line(vc_sql3);
    dbms_output.put_line(vc_sql4);
    dbms_output.put_line(vc_sql5);
    dbms_output.put_line(vc_sql6);
    dbms_output.put_line(vc_sql7);
    dbms_output.put_line(vc_sql8);
    dbms_output.put_line(vc_sql9);

    EXECUTE IMMEDIATE vc_sql1 || vc_sql2 || vc_sql3 || vc_sql4 || vc_sql5 ||
                      vc_sql6 || vc_sql7 || vc_sql8 || vc_sql9;
    COMMIT;

  END;

END PCK_AUTOTEST_BALANCE;

