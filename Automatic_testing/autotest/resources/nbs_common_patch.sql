alter table REF_COMCSI_FSQN add curr_code varchar2(3);
comment on column REF_COMCSI_FSQN.curr_code is 'Currency code';
alter table NBS_COMMON.REF_INSTRUSTFUND_AGT add ins_cal_method VARCHAR2(1);
comment on column NBS_COMMON.REF_INSTRUSTFUND_AGT.ins_cal_method is 'Insurance Calculation Method:"Manual":M/"Auto":A';
update ref_instrustfund_agt i
   set i.ins_cal_method =
       (select a.ins_cal_method
          from ref_agent a
         where a.agent_seq = i.agent_seq),
       i.last_act_typ   = 'MOD',
       i.last_act_user  = '29718',
       i.last_act_date  = sysdate;

