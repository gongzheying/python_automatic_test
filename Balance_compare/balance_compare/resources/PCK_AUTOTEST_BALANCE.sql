CREATE OR REPLACE PACKAGE PCK_AUTOTEST_BALANCE IS

PROCEDURE p_create_balance(bsp_code     in varchar2,
                             processdate  in varchar2);

PROCEDURE p_insert_balance(bsp_code          in varchar2,
                            processdate       in varchar2);

END PCK_AUTOTEST_BALANCE;
