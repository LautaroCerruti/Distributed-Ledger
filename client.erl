-module(client).

-export([start/0,stop/0,append/1,get/0]).

-export([handler/3]).

start() ->
    register(handler, spawn(?MODULE, handler, [0, nodes(hidden), []])).

stop() ->
    handler ! fin,
    unregister(handler).

handler(C, Nodos, CsList) ->
    receive
        get -> 
            lists:foreach(
                fun(X) ->
                    {clientReceiver, X} ! {getRequest, self(), C+1}
                end, Nodos),
                handler(C+1, Nodos, CsList ++ [C+1]);
        {append, Data} ->
            lists:foreach(
                fun(X) ->
                    {clientReceiver, X} ! {appendRequest, self(), C+1, Data}
                end, Nodos),
                handler(C+1, Nodos, CsList ++ [C+1]);
        {CRes, appendResACK, Data} ->
            case lists:member(CRes, CsList) of
                true -> 
                    io:format("Se agregó ~p ~n", [Data]),
                    handler(C, Nodos, lists:delete(CRes, CsList));
                false -> handler(C, Nodos, CsList)
            end;
        {CRes, getRes, Ledger} ->
            case lists:member(CRes, CsList) of
                true -> 
                    io:format("~p ~n", [Ledger]),
                    handler(C, Nodos, lists:delete(CRes, CsList));
                false -> handler(C, Nodos, CsList)
            end;
        fin -> ok
    end.

get() -> handler ! get.

append(Data) -> 
    handler ! {append, Data}.