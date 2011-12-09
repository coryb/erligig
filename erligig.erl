%% -*- mode: erlang -*-
-module(erligig).
-export([start/0]).

-include_lib("stdlib/include/qlc.hrl").

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

% create record types for out DB's
%---------------------------------
% worker record just marks if a worker socket is busy
-record(worker, {sock,state=ready}).
% function record maps the available functions onto the worker sockets
-record(function, {sock,name}).

-record(work, {id,ctime,name,data,client_id,client,assigned=null,schedule}).

start() ->
    ok = application:start(crypto),
    % start up mnesia DB
    ok = application:start(mnesia),
    % create our tables, in memory only on local node
    {atomic,ok} = mnesia:create_table(worker, [{ram_copies,[node()]},{type,set},{attributes,record_info(fields,worker)}]),
    {atomic,ok} = mnesia:create_table(function, [{ram_copies,[node()]},{type,bag},{attributes,record_info(fields,function)}]),

    % create our memory + disk tables:
    {atomic,ok} = mnesia:create_table(work, [{ram_copies,[node()]},{type,bag},{attributes,record_info(fields,work)}]),

    % make sure tables are created before we continue
    ok = mnesia:wait_for_tables([work,worker,function],infinity),

    register(logger, spawn(fun() -> logger_loop() end)),
    register(randproc,spawn(fun() -> rand_loop(crypto:rand_bytes(8092),0) end)),
    register(wakeup, spawn(fun() -> wakeup_loop() end)), 

    % bind to our listn port then start acceptint connections
    {ok, ListenSocket} = gen_tcp:listen(8888, [{active,false}, binary, {reuseaddr, true}]),
    spawn(fun() -> acceptor(ListenSocket) end),
    timer:sleep(infinity).

rand(Len) ->
    randproc ! {self(),Len},
    receive
        {ok,Data} ->
            Data
    end.

rand_loop(Data,Offset) ->
    receive
        {Pid,Len} ->
            if Len > 8092 - Offset -> 
                    Pid ! {ok,crypto:rand_bytes(Len)},
                    rand_loop(crypto:rand_bytes(8092),0);
               true ->
                    <<_:Offset/binary,Bytes:Len/binary,_/binary>> = Data,
                    Pid ! {ok, Bytes},
                    rand_loop(Data,Offset + Len)
            end
    end.

logger_loop() ->
    receive
        [Level,File,Line,Format,Args] ->
            io:format(string:concat("~s [~s:~.10B]: ", Format), [Level,File,Line|Args])
    end,
    logger_loop().

acceptor(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    spawn(fun() -> acceptor(ListenSocket) end),
    %% reset seed for so we can create uniq uuids
    %fprof:trace(start),
    handle(Socket).

dbq(QLC)->
    {atomic, Result} = mnesia:transaction(fun() -> qlc:e(QLC) end),
    Result.

handle(Socket) ->
    case gen_tcp:recv(Socket,0) of
        {ok, <<"status\r\n">>} ->
            report_status(Socket);
        {ok, <<"\0REQ", T:32, L:32, D:L/binary>>} ->
            % we might get several seperate protocol requests in one Message, so use binary comprehension to
            % split them up and call handle_message for each
            handle_message(Socket,?REQ,T,D),
            handle(Socket);
        {ok, Wha} ->
            [ handle_message(Socket,?REQ,T,D) || <<"\0REQ", T:32, L:32, D:L/binary>> <= Wha ],
            handle(Socket);
        {error, timeout} -> 
            handle(Socket);
        {error, closed} ->
            %fprof:trace(stop),
            mnesia:transaction(
              fun() -> 
                      dbq(qlc:q([mnesia:delete({function, X#function.sock}) || X <- mnesia:table(function), X#function.sock =:= Socket])),
                      dbq(qlc:q([mnesia:delete({worker, X#worker.sock})     || X <- mnesia:table(worker),   X#worker.sock =:= Socket])),
                      
                      %% if it died while work was still running on it, then update the queue so it can be issued out again    
                      Jobs = dbq(qlc:q([X#work{assigned=null} || X <- mnesia:table(work),   X#work.assigned =:= Socket])),
                      [mnesia:write(X) || X <- Jobs],
                      %% if jobs found, then kick some other worker to handle the workload
                      %% that this one just abandoned
                      [wakeup_worker(Job) || Job <- Jobs]
              end
             ),
            ok;
        UnhandledMessage ->
            ?DEBUG("unhandled message: ~p~n", [UnhandledMessage]),
            handle(Socket)
    end.

handle_message(Worker,?REQ, ?CAN_DO, Data)->
    ?DEBUG("==> worker register: ~p~n",[Data]),
    % load out new worker into the DB
    WorkerRec = #worker{sock=Worker},
    FuncRec = #function{name=Data,sock=Worker},
    mnesia:transaction(fun() -> mnesia:write(WorkerRec), mnesia:write(FuncRec) end);

handle_message(Worker,?REQ,?CANT_DO,Data) ->
    ?DEBUG("==> worker unregister: ~p~n",[Data]),
    mnesia:transaction(fun() -> mnesia:delete({function,Worker}) end);

% this is a SYNC job, it will not be inserted into the queue
handle_message(Client, ?REQ, ?SUBMIT_JOB, Data)->
    ?DEBUG("==> client: ~p SUBMIT_JOB~n", [Client]),
    submit_job(Client,Client,Data);

handle_message(Client, ?REQ, ?SUBMIT_JOB_BG, Data) ->
    ?DEBUG("==> client: ~p SUBMIT_JOB_BG~n", [Client]),
    submit_job(Client,null,Data);

handle_message(Worker,?REQ, ?GRAB_JOB,_Data)->
    ?DEBUG("==> worker: ~p GRAB_JOB~n", [Worker]),
    Work = find_work(Worker),
    if Work =:= null ->       
            ?DEBUG("<== worker: ~p NO_JOB~n", [Worker]),
            send_response(Worker,?NO_JOB,<<>>);
       true ->
            Workload = list_to_binary([Work#work.id,0,Work#work.name,0,Work#work.data]),

            % update our state to mark the worker busy so we dont send it more work
            WorkerRec = #worker{sock=Worker,state=busy},
            % also update our job queue so we dont reassign the job to someone else
            WorkRec = Work#work{assigned=Worker},
            mnesia:transaction(fun() -> mnesia:write(WorkerRec), mnesia:write(WorkRec) end),

            ?DEBUG("<== worker: ~p JOB_ASSIGN~n", [Worker]),
            send_response(Worker, ?JOB_ASSIGN, Workload)
    end;

handle_message(Worker,?REQ, ?PRE_SLEEP,_Data)->
    ?DEBUG("==> worker: ~p PRE_SLEEP~n", [Worker])
    %% do we need to update worker.state to "asleep" or
    %% do we just send NOOP RES to workers anytime
    %% we want them to do work?
    ;

handle_message(Worker,?REQ, ?WORK_COMPLETE, Data) ->
    ?DEBUG("==> worker: ~p WORK_COMPLETE~n", [Worker]),
    %% FIXME should use binary:split here, but not available in my 
    %% erlang version R13B03
    [A|_] = string:tokens(binary_to_list(Data),"\0"),
    ID = list_to_binary(A),

    % the worker is available to do work again
    WorkerRec = #worker{sock=Worker,state=ready},
    mnesia:transaction(fun() -> mnesia:write(WorkerRec) end),

    % find the job just completed
    [Job|_] = dbq(qlc:q([J || J <- mnesia:table(work),
                              J#work.id =:= ID])),
    mnesia:transaction(fun() -> mnesia:delete({work,Job#work.id}) end),

    if Job#work.client =/= null ->
            ?DEBUG("<== client: ~p WORK_COMPLETE~n", [Job#work.client]),
            send_response(Job#work.client, ?WORK_COMPLETE, Data);
       true -> noop
    end;

% this ones goes last to alert us something didnt get handled properly
handle_message(Socket, Req, Type, Data) ->
    ?DEBUG("got unknown message from sock:~p req:~p type:~p data:~p~n", [Socket,Req,Type,Data]).

send_response(Socket, Type, Data) ->
    Len=byte_size(Data),
    gen_tcp:send(Socket, [?RES, <<Type:32, Len:32>>, Data]).

submit_job(Client, RespondTo, Data) ->
    Job = mkjob(RespondTo,Data),
    mnesia:transaction(fun() -> mnesia:write(Job) end),
    ?DEBUG("<== client: ~p JOB_CREATED~n", [Client]),
    send_response(Client,?JOB_CREATED,Job#work.id),
    wakeup_worker(Job).

wakeup_loop() ->
    receive
        {Pid,Job} ->
            Pid ! ok,
            Worker = find_worker(Job),
            case Worker of
                null -> wakeup_loop();
                _ ->
                    ?DEBUG("<== worker: ~p NOOP~n", [Worker]),
                    send_response(Worker,?NOOP,<<>>),
                    wakeup_loop()
            end
    end.

wakeup_worker(Job) ->
    wakeup ! {self(),Job}.
    
find_worker(Job) ->
    Workers = dbq(qlc:q([W#worker.sock || W <- mnesia:table(worker),
                                          W#worker.state =:= ready,
                                          F <- mnesia:table(function),
                                          W#worker.sock =:= F#function.sock,
                                          F#function.name =:= Job#work.name ])),
    case Workers of
        [] -> null;
        Workers -> hd(Workers)
    end.

find_work(Worker) ->
    Now = now(),
    Jobs = dbq(qlc:keysort(2,
                           qlc:q([J || J <- mnesia:table(work),
                                       J#work.assigned =:= null,
                                       J#work.schedule =< Now,
                                       F <- mnesia:table(function),
                                       F#function.sock =:= Worker,
                                       J#work.name =:= F#function.name
                                 ]))),
    case Jobs of
        [] -> null;
        Jobs -> hd(Jobs)
    end.

findDelimiters(Bin,Delim,Lim) ->
    findDelimiters(Bin,Delim,Lim,1,[]).

findDelimiters(_Bin,_Delim,Lim,_Offset,Acc) when length(Acc) =:= Lim ->
    lists:reverse(Acc);

findDelimiters(Bin,Delim,Lim,Offset,Acc) ->
    <<_:Offset/binary,NewDelim:1/binary,_/binary>> = Bin,
    Prev = case Acc of [] -> 0; _ -> hd(Acc) + 1  end,
    if NewDelim =:= Delim ->
            findDelimiters(Bin,Delim,Lim,Offset+1,[Offset-Prev|Acc]);
       true ->
            findDelimiters(Bin,Delim,Lim,Offset+1,Acc)
    end.

mkjob(Client,Data) ->
    [L1,L2] = findDelimiters(Data,<<0>>,2),
    <<Function:L1/binary,_:1/binary,ID:L2/binary,_:1/binary,Work/binary>> = Data,
    Now = now(),
    #work{id=uuid(),ctime=Now,name=Function,data=Work,client_id=ID,client=Client,schedule=Now}.

report_status(Socket)->
    dbq(qlc:q([ ?DEBUG("report worker: ~p~n", [X]) || X <- mnesia:table(worker) ])),
    dbq(qlc:q([ ?DEBUG("report function: ~p~n", [X]) || X <- mnesia:table(function) ])),
    dbq(qlc:q([ ?DEBUG("report work: ~p~n", [X]) || X <- mnesia:table(work) ])),
    gen_tcp:send(Socket,<<"TODO\n">>).

tohex(C) when C < 10 -> C + 48;
tohex(C) -> C + 131.
    
uuid() ->
    <<A1:4,A2:4,A3:4,A4:4,A5:4,A6:4,A7:4,A8:4,
      B1:4,B2:4,B3:4,B4:4,
      C1:4,C2:4,C3:4,C4:4,
      D1:4,D2:4,D3:4,D4:4,
      E1:4,E2:4,E3:4,E4:4,E5:4,E6:4,E7:4,E8:4,E9:4,E10:4,E11:4,E12:4>> = rand(16),
    erlang:list_to_binary([
       tohex(A1),tohex(A2),tohex(A3),tohex(A4),tohex(A5),tohex(A6),tohex(A7),tohex(A8),"-",
       tohex(B1),tohex(B2),tohex(B3),tohex(B4),"-",
       tohex(C1),tohex(C2),tohex(C3),tohex(C4),"-",
       tohex(D1),tohex(D2),tohex(D3),tohex(D4),"-",
       tohex(E1),tohex(E2),tohex(E3),tohex(E4),tohex(E5),tohex(E6),
       tohex(E7),tohex(E8),tohex(E9),tohex(E10),tohex(E11),tohex(E12)
      ]).











