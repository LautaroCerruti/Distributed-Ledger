-module(aBroadcast).
-export([start/0, stop/0]).
-export([loopPQueue/3, serverReceiver/0]).
-export([atomicBroadcast/2, propose/3]).

start() ->
    register(pQueue, spawn(?MODULE, loopPQueue, [[], 0, 0])),
    register(serverReceiver, spawn(?MODULE, serverReceiver, [])).

stop() ->
    pQueue ! fin,
    serverReceiver ! fin,
    unregister(pQueue),
    unregister(serverReceiver).

queueReceiver(Q, A, P) ->
    receive
        {propose, Pid, Id, Msg} -> 
            Prop = max(A, P) + 1, 
            Pid ! {Prop, node()},
            loopPQueue(Q++[{Id, Msg, {Prop, node()}, false}], A, Prop);
        {agree, Id, Msg, {A1, A2}} ->
            loopPQueue(orderedInsert({Id, Msg, {A1, A2}, true}, lists:keydelete(Id, 1, Q)), max(A, A1), P);
        fin -> ok;
        _ -> io:format("RECV CUALCA QUEUE ~n")
    end.

loopPQueue(Q, A, P) ->
    if length(Q) /= 0 -> 
        [{Idc, Msgc, _, State}|Qt] = Q,
        if State -> 
            operationsHandler ! {Idc, Msgc},
            loopPQueue(Qt, A, P);
            true -> queueReceiver(Q, A, P)
        end;
        true -> queueReceiver(Q, A, P)
    end.

orderedInsert(V, []) ->
    [V];
orderedInsert({Id1, Msg1, A, State1}, [{Id2, Msg2, P, State2}|TL]) ->
    Comparison = proposalsCompare(A, P), 
    if
        Comparison > 0 -> [{Id2, Msg2, P, State2}] ++ orderedInsert({Id1, Msg1, A, State1}, TL);
        true -> [{Id1, Msg1, A, State1}, {Id2, Msg2, P, State2}] ++ TL
    end.

receiveProposals(0, P) ->
    P;
receiveProposals(L, A) ->
    receive
        {proposal, Who, P} ->
            monitor_node(Who, false),
            Comparison = proposalsCompare(A, P), 
            if
                Comparison > 0 -> receiveProposals(L-1, A);
                true -> receiveProposals(L-1, P)
            end;
        {nodedown, Who} ->
            io:format("SE CALLÓ ~p ~n", [Who]),
            monitor_node(Who, false),
            receiveProposals(L-1, A)
    end.

proposalsCompare({A1, A2}, {P1, P2}) ->
    if 
        A1 /= P1 -> 
            A1 - P1;
        true -> 
            [Fe|_] = lists:sort([A2, P2]),
            if A2 == Fe -> -1;
                true -> 1
            end
    end.

atomicBroadcast(Id, Msg) -> 
    pQueue ! {propose, self(), Id, Msg},
    receive
        Proposal -> 
            lists:foreach(fun (X) -> 
                            {serverReceiver, X} ! {proposeRequest, self(), Id, Msg},
                            monitor_node(X, true)
                        end, nodes()),
            AgreedValue = receiveProposals(length(nodes()), Proposal),
            pQueue ! {agree, Id, Msg, AgreedValue},
            lists:foreach(fun (X) -> 
                            {serverReceiver, X} ! {agreedRequest, Id, Msg, AgreedValue}
                        end, nodes())
    end.

propose(Pid, Id, Msg) ->
    pQueue ! {propose, self(), Id, Msg},
    receive
        Proposal -> Pid ! {proposal, node(), Proposal}
    end.

serverReceiver() ->
    receive
        {proposeRequest, Pid, Id, Msg} -> 
            spawn(?MODULE, propose, [Pid, Id, Msg]),
            serverReceiver();
        {agreedRequest, Id, Msg, AgreedValue} -> 
            pQueue ! {agree, Id, Msg, AgreedValue},
            serverReceiver();
        fin -> ok;
        _ -> io:format("RECV CUALCA SERVER~n")
    end.
