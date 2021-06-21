-module(server).
-export([start/0,stop/0]).
-export([looppqueue/3, serverReceiver/0, clientReceiver/0, loopOperations/0, loopGet/1, loopAppend/1, loopLedger/1, atomicBroadcast/2]).

start() ->
    register(pqueue, spawn(?MODULE, looppqueue, [[], 0, 0])),
    register(serverReceiver, spawn(?MODULE, serverReceiver, [])),
    register(operationsHandler, spawn(?MODULE, loopOperations, [])),
    register(getHandler, spawn(?MODULE, loopGet, [[]])),
    register(appendHandler, spawn(?MODULE, loopAppend, [[]])),
    register(ledgerHandler, spawn(?MODULE, loopLedger, [[]])),
    register(clientReceiver, spawn(?MODULE, clientReceiver, [])).

stop() ->
    pqueue ! fin,
    serverReceiver ! fin,
    operationsHandler ! fin,
    getHandler ! fin,
    appendHandler ! fin,
    ledgerHandler ! fin,
    clientReceiver ! fin,
    unregister(pqueue),
    unregister(serverReceiver),
    unregister(operationsHandler),
    unregister(getHandler),
    unregister(appendHandler),
    unregister(ledgerHandler),
    unregister(clientReceiver).

queueReceiver(Q, A, P) ->
    receive
        {propose, Pid, Id, Msj} -> 
            Prop = max(A, P) + 1, 
            Pid ! {Prop, node()},
            looppqueue(Q++[{Id, Msj, {Prop, node()}, false}], A, Prop);
        {agree, Id, Msj, {A1, A2}} ->
            looppqueue(orderedInsert({Id, Msj, {A1, A2}, true}, lists:keydelete(Id, 1, Q)), max(A, A1), P);
        fin -> ok;
        _ -> io:format("Recv cualca Queue ~n")
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
            io:format("Se calló ~p ~n", [Who]),
            monitor_node(Who, false),
            receiveProposals(L-1, A)
    end.

proposalsCompare({A1, A2}, {P1, P2}) ->
    io:format("Estoy comparando ~p y ~p ~n", [{A1, A2}, {P1, P2}]),
    if 
        A1 /= P1 -> 
            A1 - P1;
        true -> 
            [Fe|_] = lists:sort([A2, P2]),
            if A1 == Fe -> -1;
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

serverReceiver() ->
    receive
        {proposeRequest, Pid, Id, Msj} -> 
            pqueue ! {propose, self(), Id, Msj},
            receive
                Proposal -> Pid ! {proposal, node(), Proposal}
            end,
            serverReceiver();
        {agreedRequest, Id, Msj, AgreedValue} -> 
            pqueue ! {agree, Id, Msj, AgreedValue},
            serverReceiver();
        fin -> ok;
        _ -> io:format("Recv cualca server ~n")
    end.

loopGet(GetPending) ->
    receive
        {add, Id} -> loopGet([Id] ++ GetPending);
        {get, {Pid, C}} -> 
            case lists:member({Pid, C}, GetPending) of
                true ->
                    ledger ! {get, self()},
                    receive
                        Ledger -> Pid ! {C, getRes, Ledger}
                    end,
                    loopGet(lists:delete({Pid, C}, GetPending));
                false -> loopGet(GetPending)
            end;
        fin -> ok
    end.

loopAppend(AppendPendig) ->
    receive
        {add, Id} -> loopAppend([Id] ++ AppendPendig);
        {append, {Pid, C}, Data} ->
            ledger ! {append, self(), {Pid, C}, Data},
            receive
                true -> 
                    case lists:member({Pid, C}, AppendPendig) of
                       true -> 
                           Pid ! {C, appendResACK, Data},
                           loopAppend(lists:delete({Pid, C}, AppendPendig))
                    end;
                false -> loopAppend(AppendPendig)
            end;
        fin -> ok
    end.

loopLedger(Ledger) ->
    receive
        {get, Pid} -> 
            Pid ! Ledger,
            loopLedger(Ledger);
        {append, Pid, Id, Data} ->
            case lists:keyfind(Id, 1, Ledger) of
                false -> 
                    Pid ! true,
                    loopLedger(Ledger ++ [{Id, Data}]);
                _ -> 
                    Pid ! false,
                    loopLedger(Ledger)
            end;
        fin -> ok
    end.

loopOperations() ->
    receive
        {Id, get} -> 
            getHandler ! {get, Id},
            loopOperations();
        {Id, {append, Data}} -> 
            appendHandler ! {append, Id, Data},
            loopOperations();
        fin -> ok;
        _ -> ok
    end.

clientReceiver() ->
    receive
        {getRequest, Pid, C} -> 
            spawn(?MODULE, atomicBroadcast, [{Pid, C}, get]),
            getHandler ! {add, {Pid, C}},
            clientReceiver();
        {appendRequest, Pid, C, Data} -> 
            spawn(?MODULE, atomicBroadcast, [{Pid, C}, {append, Data}]),
            appendHandler ! {add, {Pid, C}},
            clientReceiver();
        fin -> ok;
        _ -> io:format("Recv cualca client ~n")
    end.
