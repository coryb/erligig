%% -*- mode: erlang -*-
-module(erligig).
-export([start/0]).

-include("erligig.hrl").

start() ->
    ok = application:start(crypto),
    register(randproc,spawn(fun() -> rand_loop(crypto:rand_bytes(8092),0) end)),

    register(logger, spawn(fun() -> logger_loop() end)),
    register(wakeup, spawn(fun() -> wakeup_loop() end)), 

    erligig_db:start(),

    % bind to our listn port then start acceptint connections
    {ok, ListenSocket} = gen_tcp:listen(8888, [{active,false}, binary, {reuseaddr, true}]),
    spawn(fun() -> acceptor(ListenSocket) end),
    timer:sleep(infinity).

rand(Len) ->
    randproc ! {self(),Len},
    receive {ok,Data} -> Data end.

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
    %fprof:trace(start),
    handle(Socket).

handle(Socket) ->
    case gen_tcp:recv(Socket,0) of
        {ok, <<"status\r\n">>} ->
            report_status(Socket);
        {ok, <<"prof start\r\n">>} ->
            fprof:trace(start),
            gen_tcp:send(Socket,<<"Fine\n">>);
        {ok, <<"prof stop\r\n">>} ->
            fprof:trace(stop),
            gen_tcp:send(Socket,<<"Whatevs\n">>);
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
            Jobs = erligig_db:worker_gone(Socket),
            [wakeup_worker(Job) || Job <- Jobs];
        UnhandledMessage ->
            ?DEBUG("unhandled message: ~p~n", [UnhandledMessage]),
            handle(Socket)
    end.

handle_message(Worker,?REQ, ?CAN_DO, Data)->
    ?DEBUG("==> worker register: ~p~n",[Data]),
    % load out new worker into the DB
    erligig_db:worker_register(Worker,Data);

handle_message(Worker,?REQ,?CANT_DO,Data) ->
    ?DEBUG("==> worker unregister: ~p~n",[Data]),
    erligig_db:worker_unregister(Worker);

% this is a SYNC job, it will not be inserted into the queue
handle_message(Client, ?REQ, ?SUBMIT_JOB, Data)->
    ?DEBUG("==> client: ~p SUBMIT_JOB~n", [Client]),
    submit_job(Client,Client,Data);

handle_message(Client, ?REQ, ?SUBMIT_JOB_BG, Data) ->
    ?DEBUG("==> client: ~p SUBMIT_JOB_BG~n", [Client]),
    submit_job(Client,null,Data);

handle_message(Worker,?REQ, ?GRAB_JOB,_Data)->
    ?DEBUG("==> worker: ~p GRAB_JOB~n", [Worker]),
    Work = erligig_db:assign_work(Worker),
    if Work =:= null ->       
            ?DEBUG("<== worker: ~p NO_JOB~n", [Worker]),
            send_response(Worker,?NO_JOB,<<>>);
       true ->
            Workload = list_to_binary([Work#work.id,0,Work#work.name,0,Work#work.data]),
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

    case erligig_db:work_done(Worker,ID) of
        null -> ok;
        Client -> 
            ?DEBUG("<== client: ~p WORK_COMPLETE~n", []),
            send_response(Client, ?WORK_COMPLETE, Data)
    end;

% this ones goes last to alert us something didnt get handled properly
handle_message(Socket, Req, Type, Data) ->
    ?DEBUG("got unknown message from sock:~p req:~p type:~p data:~p~n", [Socket,Req,Type,Data]).

send_response(Socket, Type, Data) ->
    Len=byte_size(Data),
    gen_tcp:send(Socket, [?RES, <<Type:32, Len:32>>, Data]).

submit_job(Client, RespondTo, Data) ->
    Job = mkjob(RespondTo,Data),
    erligig_db:add_work(Job),

    ?DEBUG("<== client: ~p JOB_CREATED~n", [Client]),
    send_response(Client,?JOB_CREATED,Job#work.id),
    wakeup_worker(Job).

wakeup_loop() ->
    receive
        {Pid,Job} ->
            Pid ! ok,
            Worker = erligig_db:available_worker(Job),
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
    #work{schedule=Now,id=uuid(),ctime=Now,name=Function,data=Work,client_id=ID,client=Client}.

report_status(Socket)->
    [ ?DEBUG("report worker: ~p~n", [X]) || X <- erligig_db:all_workers() ],
    [ ?DEBUG("report function: ~p~n", [X]) || X <- erligig_db:all_functions() ],
    [ ?DEBUG("report work: ~p~n", [X]) || X <- erligig_db:all_work() ],
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
