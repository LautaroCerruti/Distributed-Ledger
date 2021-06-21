-module(atomicbroadcast).
-export([start/0, stop/0]).
-export([looppqueue/3, serverReceiver/0]).
-export([atomicBroadcast/2, propose/3]).

start() ->
    register(pqueue, spawn(?MODULE, looppqueue, [[], 0, 0])),
    register(serverReceiver, spawn(?MODULE, serverReceiver, [])).

stop() ->
    pqueue ! fin,
    serverReceiver ! fin,
    unregister(pqueue),
    unregister(serverReceiver).

queueReceiver(Q, A, P) ->
    receive
        {propose, Pid, Id, Msj} -> 
            Prop = max(A, P) + 1, 
            Pid ! {Prop, node()},
            looppqueue(Q++[{Id, Msj, {Prop, node()}, false}], A, Prop);
        {agree, Id, Msj, {A1, A2}} ->
            looppqueue(orderedInsert({Id, Msj, {A1, A2}, true}, lists:keydelete(Id, 1, Q)), max(A, A1), P);
        fin -> ok;
        _ -> io:format("RECV CUALCA QUEUE ~n")
    end.

looppqueue(Q, A, P) ->
    if length(Q) /= 0 -> 
        [{Idc, Msjc, _, State}|Qt] = Q,
        if State -> 
            operationsHandler ! {Idc, Msjc},
            looppqueue(Qt, A, P);
            true -> queueReceiver(Q, A, P)
        end;
        true -> queueReceiver(Q, A, P)
    end.

orderedInsert(V, []) ->
    [V];
orderedInsert({Id1, Msj1, A, State1}, [{Id2, Msj2, P, State2}|TL]) ->
    Comparison = proposalsCompare(A, P), 
    if
        Comparison > 0 -> [{Id2, Msj2, P, State2}] ++ orderedInsert({Id1, Msj1, A, State1}, TL);
        true -> [{Id1, Msj1, A, State1}, {Id2, Msj2, P, State2}] ++ TL
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

atomicBroadcast(Id, Msj) -> 
    pqueue ! {propose, self(), Id, Msj},
    receive
        Proposal -> 
            lists:foreach(fun (X) -> 
                            {serverReceiver, X} ! {proposeRequest, self(), Id, Msj},
                            monitor_node(X, true)
                        end, nodes()),
            AgreedValue = receiveProposals(length(nodes()), Proposal),
            pqueue ! {agree, Id, Msj, AgreedValue},
            lists:foreach(fun (X) -> 
                            {serverReceiver, X} ! {agreedRequest, Id, Msj, AgreedValue}
                        end, nodes())
    end.

propose(Pid, Id, Msj) ->
    pqueue ! {propose, self(), Id, Msj},
    receive
        Proposal -> Pid ! {proposal, node(), Proposal}
    end.

serverReceiver() ->
    receive
        {proposeRequest, Pid, Id, Msj} -> 
            spawn(?MODULE, propose, [Pid, Id, Msj]),
            serverReceiver();
        {agreedRequest, Id, Msj, AgreedValue} -> 
            pqueue ! {agree, Id, Msj, AgreedValue},
            serverReceiver();
        fin -> ok;
        _ -> io:format("RECV CUALCA SERVER~n")
    end.
