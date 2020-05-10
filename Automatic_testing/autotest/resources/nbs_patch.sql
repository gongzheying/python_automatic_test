alter table STA_MISYSALES add bop_flag VARCHAR2(1) default 'N';
comment on column STA_MISYSALES.bop_flag is 'BOP Flag. ''Y''/''N''.';
