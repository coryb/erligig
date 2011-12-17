%% -*- mode: erlang -*-
-module(erligig_db).
-export([start/0, worker_gone/1, worker_register/2, worker_unregister/1, assign_work/1, work_done/2, add_work/1, available_worker/1, all_workers/0, all_functions/0, all_work/0]).

-include("erligig.hrl").

-include_lib("stdlib/include/qlc.hrl").

start() ->
    %% start up mnesia DB
    ok = application:start(mnesia),

    %% create our tables, in memory only on local node
    {atomic,ok} = mnesia:create_table(worker, [{ram_copies,[node()]},{type,set},{attributes,record_info(fields,worker)}]),
    {atomic,ok} = mnesia:create_table(function, [{ram_copies,[node()]},{type,bag},{attributes,record_info(fields,function)}]),

    %% create our memory + disk tables:
    {atomic,ok} = mnesia:create_table(work, [{ram_copies,[node()]},{type,ordered_set},{attributes,record_info(fields,work)},{index,[id]}]),

    %% make sure tables are created before we continue
    ok = mnesia:wait_for_tables([work,worker,function],infinity),

    register(db, spawn(fun() -> db_loop() end)),
    ok.

dbq(QLC)->
    {atomic, Result} = mnesia:transaction(fun() -> qlc:e(QLC) end),
    Result.

dbc(QH,Count)->
    Wtf = mnesia:transaction(
                         fun() -> 
                                 QC = qlc:cursor(QH),
                                 Result = qlc:next_answers(QC,Count),
                                 qlc:delete_cursor(QC),
                                 Result
                         end),
    {atomic, Result} = Wtf,
    Result.

worker_gone(Worker) ->
    {atomic, Results} = mnesia:transaction(
      fun() -> 
              qlc:e(qlc:q([mnesia:delete({function, X#function.sock}) || X <- mnesia:table(function), X#function.sock =:= Worker])),
              qlc:e(qlc:q([mnesia:delete({worker, X#worker.sock})     || X <- mnesia:table(worker),   X#worker.sock =:= Worker])),
              
              %% if it died while work was still running on it, then update the queue so it can be issued out again    
              Jobs = qlc:e(qlc:q([X#work{assigned=null} || X <- mnesia:table(work),   X#work.assigned =:= Worker])),
              [mnesia:write(X) || X <- Jobs],
              %% if jobs found, then kick some other worker to handle the workload
              %% that this one just abandoned
              Jobs
      end
     ),
    Results.

worker_register(Worker,Function) ->
    WorkerRec = #worker{sock=Worker},
    FuncRec = #function{name=Function,sock=Worker},
    mnesia:transaction(fun() -> mnesia:write(WorkerRec), mnesia:write(FuncRec) end).

worker_unregister(Worker) ->
    mnesia:transaction(fun() -> mnesia:delete({function,Worker}) end).

assign_work(Worker) ->
    assign_work(Worker, mnesia:dirty_first(work)).

assign_work(_Worker,'$end_of_table') -> null;
assign_work(Worker,JobId) -> 
    Now = now(),
    case mnesia:dirty_read({work,JobId}) of
        %% if we find a schedule after now, all the rest will
        %% also be after now, so give up
        [Job] when Job#work.schedule > Now ->
            null;
        [Job] when Job#work.assigned =:= null ->
            QH = qlc:q([F || F <- mnesia:table(function),
                             F#function.sock =:= Worker,
                             F#function.name =:= Job#work.name]),
            case dbc(QH,1) of
                [] ->        
                    assign_work(mnesia:dirty_next(work));
                [_] -> 
                    WorkerRec = #worker{sock=Worker,state=busy},
                    %% also update our job queue so we dont reassign the job to someone else
                    WorkRec = Job#work{assigned=Worker},
                    mnesia:dirty_write(WorkerRec),
                    mnesia:dirty_write(WorkRec),
                    WorkRec
            end;
        Wha ->
            ?DEBUG("Got wha: ~p~n",[Wha]),
            null
    end.

    %% Job = mnesia:dirty_read({work,JobId}),
    
    
   


    %% Now = now(),
    %% QH = qlc:q([J || J <- mnesia:table(work),
    %%                  J#work.schedule =< Now,
    %%                  J#work.assigned =:= null,
    %%                  F <- mnesia:table(function),
    %%                  F#function.sock =:= Worker,
    %%                  J#work.name =:= F#function.name
    %%            ]),
    %% case dbc(QH,1) of
    %%     [] -> null;
    %%     [Work] ->
    %%         %% update our state to mark the worker busy so we dont send it more work
    %%         WorkerRec = #worker{sock=Worker,state=busy},
    %%         %% also update our job queue so we dont reassign the job to someone else
    %%         WorkRec = Work#work{assigned=Worker},
    %%         mnesia:transaction(fun() -> mnesia:write(WorkerRec), mnesia:write(WorkRec) end),
    %%         WorkRec
    %% end.

work_done(Worker,ID) ->
    %% the worker is available to do work again
    WorkerRec = #worker{sock=Worker,state=ready},
    mnesia:dirty_write(WorkerRec),
    %% mnesia:transaction(fun() -> mnesia:write(WorkerRec) end),
    
    %% %% find the job just completed
    %% QH = qlc:q([J || J <- mnesia:table(work),
    %%                           J#work.id =:= ID]),
    %% [Job] = dbc(QH,1),
    %% mnesia:transaction(fun() -> mnesia:delete_object(Job) end),
    case mnesia:dirty_index_read(work,ID,#work.id) of
        [] -> null;
        [Job] -> mnesia:dirty_delete_object(Job),
                 Job#work.client
    end.

add_work(Work) ->
    mnesia:dirty_write(Work).
    %%mnesia:transaction(fun() -> mnesia:write(Work) end).

available_worker(Work) ->
    QH = qlc:q([W#worker.sock || W <- mnesia:table(worker),
                                 W#worker.state =:= ready,
                                 F <- mnesia:table(function),
                                 W#worker.sock =:= F#function.sock,
                                 F#function.name =:= Work#work.name ]),
    case dbc(QH,1) of
        [] -> null;
        [Worker] -> Worker#worker.sock
    end.

all_workers() ->
    dbq(qlc:q([ X || X <- mnesia:table(worker) ])).
all_functions() ->
    dbq(qlc:q([ X || X <- mnesia:table(function) ])).
all_work() ->
    dbq(qlc:q([ X || X <- mnesia:table(work) ])).

db_loop() ->
    receive
        {Pid,insert_job,Rec} ->
            Pid ! mnesia:dirty_write(Rec),
            db_loop();
        {Pid,Tab,Key} ->
            Pid ! mnesia:dirty_delete(Tab,Key),
            db_loop()
    end.

