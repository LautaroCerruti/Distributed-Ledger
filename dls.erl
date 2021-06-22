-module(dls).
-import(aBroadcast, [atomicBroadcast/2]).
-export([start/0, stop/0]).
-export([clientReceiver/0, loopOperations/0, loopGet/1, loopAppend/1, loopLedger/1]).
-export([makeAppend/2, makeGet/1]).

start() ->
    register(msgsHandler, spawn(?MODULE, loopOperations, [])),
    register(getHandler, spawn(?MODULE, loopGet, [[]])),
    register(appendHandler, spawn(?MODULE, loopAppend, [[]])),
    register(ledgerHandler, spawn(?MODULE, loopLedger, [[]])),
    register(clientReceiver, spawn(?MODULE, clientReceiver, [])).

stop() ->
    msgsHandler ! fin,
    getHandler ! fin,
    appendHandler ! fin,
    ledgerHandler ! fin,
    clientReceiver ! fin,
    unregister(msgsHandler),
    unregister(getHandler),
    unregister(appendHandler),
    unregister(ledgerHandler),
    unregister(clientReceiver).

makeGet({Pid, C}) ->
    ledgerHandler ! {get, self()},
    receive
        Ledger -> Pid ! {C, getRes, Ledger}
    end.


loopGet(GetPending) ->
    receive
        {add, Id} -> loopGet([Id] ++ GetPending);
        {get, Id} -> 
            case lists:member(Id, GetPending) of
                true ->
                    spawn(?MODULE, makeGet, [Id]),
                    loopGet(lists:delete(Id, GetPending));
                false -> loopGet(GetPending)
            end;
        fin -> ok;
        _ -> loopGet(GetPending)
    end.

makeAppend(Id, Data) ->
    ledgerHandler ! {append, self(), Id, Data},
    receive
        true -> 
            appendHandler ! {delete, Id, Data};
        false -> ok
    end.

loopAppend(AppendPending) ->
    receive
        {add, Id} -> loopAppend([Id] ++ AppendPending);
        {append, Id, Data} ->
            spawn(?MODULE, makeAppend, [Id, Data]),
            loopAppend(AppendPending);
        {delete, {Pid, C}, Data} ->
            case lists:member({Pid, C}, AppendPending) of
                true -> 
                    Pid ! {C, appendResACK, Data},
                    loopAppend(lists:delete({Pid, C}, AppendPending));
                false -> loopAppend(AppendPending)
            end;
        fin -> ok;
        _ -> loopAppend(AppendPending)
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
        fin -> ok;
        _ -> loopLedger(Ledger)
    end.

loopOperations() ->
    receive
        {{Pid, C, _}, get} -> 
            getHandler ! {get, {Pid, C}},
            loopOperations();
        {{Pid, C, _}, {append, Data}} -> 
            appendHandler ! {append, {Pid, C}, Data},
            loopOperations();
        fin -> ok;
        _ -> loopOperations()
    end.

clientReceiver() ->
    receive
        {getRequest, Pid, C} -> 
            spawn(aBroadcast, atomicBroadcast, [{{Pid, C}, node()}, get]),
            getHandler ! {add, {Pid, C}},
            clientReceiver();
        {appendRequest, Pid, C, Data} -> 
            spawn(aBroadcast, atomicBroadcast, [{{Pid, C}, node()}, {append, Data}]),
            appendHandler ! {add, {Pid, C}},
            clientReceiver();
        {nodeListRequest, Pid}-> 
            Pid ! {nodeListRes, nodes()},
            clientReceiver();
        fin -> ok;
        _ -> 
            io:format("RECV CUALCA CLIENT ~n"),
            clientReceiver()
    end.
