-module(yredis_worker).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API
-export([start_link/5, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {server, port, database, password, conn}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Shard, Server, Port, Database, Password) ->
    gen_server:start_link(?MODULE, [Shard, Server, Port, Database, Password], []).

%%--------------------------------------------------------------------
%% @spec stop() -> ok
%% @doc Stops the server
%% @end
%%--------------------------------------------------------------------
stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init([Shard, Server, Port, Database, Password]) ->
    gen_server:cast(yredis, {add, Shard, self()}),
    {ok, C} = eredis:start_link(Server, Port, Database, Password),
    {ok, #state{server=Server, port=Port, database=Database, password=Password, conn=C}}.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call({smembers, Key}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["SMEMBERS", Key]) of
        {ok, Reply} -> {reply, Reply, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, [], State}
    end;
handle_call({srem, Key, Value}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["SREM", Key, Value]) of
        {ok, _} -> {reply, ok, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, error, State}
    end;
handle_call({sadd, Key, Value}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["SADD", Key, Value]) of
        {ok, _} -> {reply, ok, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, error, State}
    end;
handle_call({multi, Transaction}, _From, #state{conn=C}=State) ->
    eredis:q(C, ["MULTI"]),
    lists:map(fun(X) -> eredis:q(C, X) end, Transaction),
    case eredis:q(C, ["EXEC"]) of
        {ok, Reply} -> {reply, Reply, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, [], State}
    end;
handle_call({command, Command, Default}, _From, #state{conn=C}=State) ->
    case eredis:q(C, Command) of
        {ok, undefined} -> {reply, Default, State};
        {ok, Result} -> {reply, Result, State};
        _ -> 
            error_logger:error_msg("Can't connect to redis server", []),
            {reply, error, State}
    end;
handle_call({get, Key}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["GET", Key]) of
        {ok, Reply} -> {reply, Reply, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, [], State}
    end;
handle_call({set, Key, Value}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["SET", Key, Value]) of
        {ok, <<"OK">>} -> {reply, ok, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            {reply, error, State}
    end;
handle_call({hget, Key, Name, Default}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["HGET", Key, Name]) of
        {ok, undefined} -> {reply, Default, State};
        {ok, Reply} -> {reply, Reply, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []),
            {reply, error, State}
    end;
handle_call({hset, Key, Name, Value}, _From, #state{conn=C}=State) ->
    case eredis:q(C, ["HSET", Key, Name, Value]) of
        {ok, <<"OK">>} -> {reply, ok, State};
        _ ->
            error_logger:error_msg("Can't connect to redis server", []),
            {reply, error, State}
    end;
handle_call(_Request, _From, State) ->
    Reply = noproc,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({set, Key, Value}, #state{conn=C}=State) ->
    case eredis:q(C, ["SET", Key, Value]) of
        {ok, <<"OK">>} -> ok;
        _ ->
            error_logger:error_msg("Can't connect to redis server", []), 
            error
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    eredis:stop(State#state.conn),
    ok.

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
