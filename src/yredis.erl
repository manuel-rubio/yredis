-module(yredis).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API
-export([start_link/1, stop/0, multi/2, getter/2, setter/3, set_add/3, set_rem/3, set_members/2, command/2, command/3, hget/4, hset/4, get_shards/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type pid_pool() :: list(pid()).
-type shard_config() :: {shard_config(), pid_pool()}.

-record(state, {
    pool = [] :: list(shard_config()),
    shards = [] :: list(binary())
}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Pool) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Pool, []).

%%--------------------------------------------------------------------
%% @spec stop() -> ok
%% @doc Stops the server
%% @end
%%--------------------------------------------------------------------
stop() ->
    gen_server:call(?SERVER, stop).

%%--------------------------------------------------------------------
%% @spec multi() -> ok
%% @doc Begin a transaction
%% @end
%%--------------------------------------------------------------------
multi(_Shard, []) ->
    ok;
multi(Shard, Transaction) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {multi, Transaction}).

command(Shard, Command) ->
    command(Shard, Command, undefined).

command(Shard, Command, Default) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {command, Command, Default}).


%%--------------------------------------------------------------------
%% @spec getter() -> [Binary1, Binary2, ..., BinaryN] | Binary
%% @doc Gets a value, given by its key
%% @end
%%--------------------------------------------------------------------
getter(Shard, Key) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {get, Key}).

hget(Shard, Key, Name, Default) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {hget, Key, Name, Default}).

hset(Shard, Key, Name, Value) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {hset, Key, Name, Value}).

set_add(Shard, Key, Value) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {sadd, Key, Value}).

set_rem(Shard, Key, Value) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {srem, Key, Value}).

set_members(Shard, Key) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {smembers, Key}).

%%--------------------------------------------------------------------
%% @spec setter() -> ok
%% @doc Sets a given value for a given key
%% @end
%%--------------------------------------------------------------------
setter(Shard, Key, Value) ->
    {ok, Worker} = gen_server:call(?SERVER, {worker, Shard}),
    gen_server:call(Worker, {set, Key, Value}).

get_shards() ->
    gen_server:call(?SERVER, shards).

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

-type shard() :: binary().

-type eredis_config() :: {
    Host::string(), Port::integer(), Database::integer(),
    Password::integer(), PoolSize::integer()
}.

-spec init(Data::list({shard(), eredis_config()}), State::#state{}) ->
    {ok, State::#state{}} | 
    {ok, State::#state{}, Timeout::(integer() | infinity)} |
     ignore | {stop, Reason::term()}.

init([], State) ->
    {ok, State};
init([{Shard, {Server, Port, Database, Password, PoolSize}}|TailPool], State) ->
    yredis_sup:start_link(),
    Pool = lists:map(fun(_X) ->
        {ok, Pid} = yredis_sup:add_worker(Shard, Server, Port, Database, Password),
        Pid
    end, lists:seq(1, PoolSize)),
    Shards = State#state.shards ++ [Shard],
    init(TailPool, #state{pool = [{Shard, Pool}|State#state.pool], shards=Shards}).

-spec init(Data::list({shard(), eredis_config()})) ->
    {ok, State::#state{}} | 
    {ok, State::#state{}, Timeout::(non_neg_integer() | hibernate | infinity)} |
    {stop, Reason::term()} |
     ignore | {stop, Reason::term()}.

init(Data) ->
    init(Data, #state{pool=[]}).

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
handle_call({worker, Shard}, From, #state{pool=Pool}=State) ->
    case proplists:get_value(Shard, Pool) of
        [C|T] ->
            {reply, {ok, C}, State#state{
                pool=proplists:delete(Shard, Pool) ++ [{Shard, T ++ [C]}]
            }};
        undefined when Shard =:= <<"default">> ->
            {reply, error, State};
        undefined ->
            handle_call({worker, <<"default">>}, From, State)
    end;
handle_call(shards, _From, State) ->
    {reply, State#state.shards, State};
handle_call(_Msg, _From, State) ->
    {reply, noproc, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({add, Shard, Pid}, #state{pool=Pool}) ->
    {noreply, #state{pool=check_pools({Shard, Pid}, Pool, [])}};
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
    lists:foreach(fun({_Shard, Pool}) ->
        [ gen_server:call(X, stop) || X <- Pool ]
    end, State#state.pool),
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

check_pools(_, [], Pools) ->
    Pools;
check_pools({Shard, Pid}=New, [{Shard, Pool}|T], Pools) ->
    NewPool = {Shard, [ X || X <- [Pid|Pool], erlang:is_process_alive(X) ]},
    check_pools(New, T, [NewPool|Pools]);
check_pools(New, [{Shard, Pool}|T], Pools) ->
    NewPool = {Shard, [ X || X <- Pool, erlang:is_process_alive(X) ]},
    check_pools(New, T, [NewPool|Pools]).

