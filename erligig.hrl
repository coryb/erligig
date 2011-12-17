-record(worker, {sock,state=ready}).
-record(function, {sock,name}).
-record(work, {schedule,id,ctime,name,data,client_id,client,assigned=null}).

-define(REQ,<<"\0REQ">>).
-define(RES,<<"\0RES">>).
-define(CAN_DO, 1).
-define(CANT_DO,2).
-define(RESET_ABILITIES,3).
-define(PRE_SLEEP,4).
-define(NOOP,6).
-define(SUBMIT_JOB, 7).
-define(JOB_CREATED, 8).  %response from SUBMIT_JOB*
-define(GRAB_JOB,9).
-define(NO_JOB,10). %response from GRAB_JOB on empty queue
-define(JOB_ASSIGN,11). %response from GRAB_JOB with work
-define(WORK_COMPLETE,13).
-define(SUBMIT_JOB_BG,18).

-define(DEBUG(Format,Args),logger ! ["DEBUG",?FILE,?LINE,Format,Args]).
%-define(DEBUG(Format,Args),ok).


