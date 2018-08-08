defmodule Firenest.ReplicatedState do
  @moduledoc """
  Distributed key-value store for ephemeral data.

  The key-value pairs are always attached to a lifetime of a
  process and the state is replicated to all nodes across the
  topology. When the attached process dies the state will be
  removed on all nodes and in case the all the state from
  disconnected nodes will be (temporarily) removed. The state
  can only be attached to local processes.

  The state is managed through callbacks that are invoked on the
  node where the process lives and remotely on other nodes when local
  changes are propagated.

  The state is replicated incrementally through building local "deltas"
  (changes to the state) and periodically replicating them remotely. Only
  some amount of recent deltas are retained for "catching up" remote nodes.
  If remote node is too far behind the current state and a delta-based
  catch-up can't be performed, the server falls back to transferring the entire
  local state. If keeping track of incremental changes is not convenient
  for a particular state type, the value of a delta can be set to be equal
  to the current state - this will always cause full state transfers.
  """
  alias Firenest.SyncedServer

  @type server() :: atom()
  @type key() :: term()

  @type local_delta() :: term()
  @type remote_delta() :: term()
  @type state() :: term()
  @type config() :: term()

  @type extra_action :: :delete | {:update_after, update :: term(), time :: pos_integer()}

  @doc """
  Called when a partition starts up.

  It returns an `initial_delta` that will be passed to `c:local_put/3`
  callback on new entries and to `c:local_update/4` after remote
  broadcast resets the local delta.
  It also returns an immutable `config` value that will be passed to
  all callbacks.
  """
  @callback init(opts :: keyword()) :: {initial_delta :: local_delta(), config()}

  @doc """
  Called whenever the `put/4` function is called to create a new state.

  The `arg` is received from the corresponding `put/4` call. For the
  explanation of the `extra_action` return values see the
  `c:local_update/4` callback.

  The value of `local_delta` argument is always the initial delta as
  returned by the `init/1` callback.

  This is a good place for broadcasting local state changes.
  """
  @callback local_put(arg :: term(), local_delta(), config()) ::
              {local_delta(), initial_state} | {local_delta(), initial_state, extra_action()}
            when initial_state: state()

  @doc """
  Called whenever the `update/4` function is called to update a state.

  This is a good place for broadcasting local state changes.

  The `local_delta` argument is either the accumulated delta from
  `c:local_put/3` and `c:local_update/4` calls or the initial delta
  from `c:init/1` after remote broadcast resets the local delta.

  ## Delayed update and delete

  If the function returns a third element in the tuple consisting of
  `{:update_after, update, time, update}`, a timer is started by the
  server and the `c:local_update/4` callback will be called with the
  `update` value after `time` milliseconds.

  If the third element is `:delete`, the state will be immediately
  deleted and the `c:local_delete/2` callback triggered.
  """
  @callback local_update(update :: term(), local_delta(), state(), config()) ::
              {local_delta(), state()} | {local_delta(), state(), extra_action()}

  @doc """
  Called whenever attached process dies or the `delete/3` or `delete/2`
  function is called and state is about to be deleted.

  The return value is ignored.

  This is a good place for broadcasting local state changes.
  """
  @callback local_delete(state(), config()) :: term()

  @doc """
  Called whenever the server is about to incrementally replicate local
  state to a remote node.

  It takes in the local delta value constructed in the `c:local_update/4`
  calls and returns a remote delta value that will be replicated to other
  servers. The value of the local delta is reset to the initial delta value
  returned from the `c:local_join/2` callback.

  In case the callback is not provided it defaults to just returning local delta.
  """
  @callback prepare_remote_delta(local_delta(), config()) :: remote_delta()

  @doc """
  Called whenever a remote delta is received from another node.

  The `remote_delta` value is the return value of the `c:prepare_remote_delta/2`
  callback. The result of applying the remote delta to state must be
  exactly the same as the result of applying local updates to the
  state in the `c:local_update/3` callback.
  """
  @callback handle_remote_delta(remote_delta(), state(), config()) :: state()

  @doc """
  Called when remote changes are received by the local server.

  It receives a list of observed remote changes for all the tracked keys.
  A `process_state_change` specifies how the state for a single process changed.
  In particular the change can be:

    * `{initial_state, [:put, ...]}` in case the process just joined
    * `{last_known_state, [..., :delete]}` in case the process just left
    * `{last_known_state, [{:replace, state}, ...]}` in case it was not possible
      to replicate incremental changes to remote state through deltas and a full
      state transfer was performed.
    * `{last_known_state, [..., {:delta, remote_delta}, ...]}` in case
      incremental changes to the remote state were communicated.

  This callback is optional and its behaviour depends on the value
  of the `:remote_changes` option provided when the server is started.

    * `:ignore` - the callback is not invoked and the server skips
      all operations required for tracking the changes. This is the
      default and should be chosen if precise state change tracking
      is not required.

    * `:observe_full` - calls the callback with as precise information
      about the remote state changes as possible. This means that, for
      example, for a very short-lived process a `[:put, :delete]` sequence
      of changes is possible.

    * `:observe_collapsed` - collapses the information about remote state
      changes. For example a `[:put, {:delta, ...}]` sequence is collapsed
      into just `[:put]` with all the state changes already applied and a
      `[{:delta, ...}, :delete]` sequence is collapsed into just `[:delete]`.
      This also means that for short-lived processes, no information
      may be transferred if the changes would contain both `:put` and `:delete`.

  This is a good place for broadcasting remote state changes.

  The return value is ignored.
  """
  @callback observe_remote_changes(observed_remote_changes, config()) :: term()
            when observed_remote_changes: [{key(), [process_state_change]}],
                 process_state_change: {current_state :: state(), [change]},
                 change:
                   :put | {:replace, new_state :: state()} | {:delta, remote_delta()} | :delete

  @optional_callbacks [observe_remote_changes: 2, prepare_remote_delta: 2]

  @doc """
  Returns a child spec for the replicated state server.

  Once started, each partition will call the `c:init/1` callback
  passing all the provided options.

  ## Options

    * `:name` - name for the process, required;
    * `:topology` - name of the supporting topology, required;
    * `:partitions` - number of partitions, defaults to 1;
    * `:broadcast_timeout` - delay of broadcasting local events to other nodes,
      defaults to 50 ms;
    * `:remote_changes` - specifies behaviour of the `c:observe_remote_changes/2`
      callback and can be `:observe_full | :observe_collapsed | :ignore`,
      defaults to `:ignore`;

  """
  defdelegate child_spec(opts), to: Firenest.ReplicatedState.Supervisor

  @doc """
  Puts new state under `key` attached to lifetime of `pid`.

  A special value `:partition` can be used for `pid` to indicate that the
  lifetime of the value will be attached to the partition itself and
  should be managed manually through `delete/3`, `delete/2` and `update/4`
  calls with `:delete` returns from the `c:local_update/4` callback.

  This calls the `c:local_put/3` callback with `arg` inside the server.
  """
  @spec put(server(), key(), pid() | :partition, term()) :: :ok | {:error, :already_joined}
  def put(server, key, pid, arg) when pid == :parittion or node(pid) == node() do
    partition = partition_info!(server, key)
    SyncedServer.call(partition, {:put, key, pid, arg})
  end

  @doc """
  Deletes state under `key` attached to lifetime of `pid`.

  For the explanation of `:partition` value for `pid` see `put/4`.

  This calls the `c:local_delete/2` callback inside the server.
  """
  @spec delete(server(), key(), pid() | :partition) :: :ok | {:error, :not_member}
  def delete(server, key, pid) when pid == :partition or node(pid) == node() do
    partition = partition_info!(server, key)
    SyncedServer.call(partition, {:delete, key, pid})
  end

  @doc """
  Deletes state under all keys attached to lifetime of `pid`.

  This calls the `c:local_delete/2` callback inside the server for
  each key the process is leaving.
  """
  @spec delete(server(), pid()) :: :ok | {:error, :not_member}
  def delete(server, pid) when node(pid) == node() do
    partitions = partition_infos!(server)
    replies = multicall(partitions, {:delete, pid}, 5_000)

    if :ok in replies do
      :ok
    else
      {:error, :not_member}
    end
  end

  @doc """
  Updates state under `key` attached to lifetime of `pid`.

  For the explanation of `:partition` value for `pid` see `put/4`.

  This calls the `c:local_update/4` callback inside the server passing
  the value of `update`.
  """
  @spec update(server(), key(), pid() | :partition, term()) :: :ok | {:error, :not_member}
  def update(server, key, pid, update) when pid == :partition or node(pid) == node() do
    partition = partition_info!(server, key)
    SyncedServer.call(partition, {:update, key, pid, update})
  end

  @doc """
  Lists all states present for a given `key`.
  """
  @spec list(server(), key()) :: [state()]
  def list(server, key) do
    partition = partition_info!(server, key)
    SyncedServer.call(partition, {:list, key}).()
  end

  # TODO
  # def dirty_list(server, group)

  defp multicall(servers, request, timeout) do
    servers
    |> Enum.map(fn server ->
      pid = Process.whereis(server)
      ref = Process.monitor(pid)
      send(pid, {:"$gen_call", {self(), ref}, request})
      ref
    end)
    |> Enum.map(fn ref ->
      receive do
        {^ref, reply} ->
          Process.demonitor(ref, [:flush])
          reply

        {:DOWN, ^ref, _, _, reason} ->
          exit(reason)
      after
        timeout ->
          Process.demonitor(ref, [:flush])
          exit(:timeout)
      end
    end)
  end

  defp partition_info!(server, key) do
    hash = :erlang.phash2(key)
    extract = {:element, {:+, {:rem, {:const, hash}, :"$1"}, 1}, :"$2"}
    ms = [{{:partitions, :"$1", :"$2"}, [], [extract]}]
    [info] = :ets.select(server, ms)
    info
  catch
    :error, :badarg ->
      raise ArgumentError, "unknown key: #{inspect(server)}"
  end

  defp partition_infos!(server) do
    Tuple.to_list(:ets.lookup_element(server, :partitions, 3))
  catch
    :error, :badarg ->
      raise ArgumentError, "unknown group: #{inspect(server)}"
  end
end

defmodule Firenest.ReplicatedState.Supervisor do
  @moduledoc false
  use Supervisor

  def child_spec(opts) do
    partitions = Keyword.get(opts, :partitions, 1)
    name = Keyword.fetch!(opts, :name)
    topology = Keyword.fetch!(opts, :topology)
    handler = Keyword.fetch!(opts, :handler)
    supervisor = Module.concat(name, "Supervisor")
    arg = {partitions, name, topology, handler, opts}

    %{
      id: __MODULE__,
      start: {Supervisor, :start_link, [__MODULE__, arg, [name: supervisor]]},
      type: :supervisor
    }
  end

  def init({partitions, name, topology, handler, opts}) do
    names =
      for partition <- 0..(partitions - 1),
          do: Module.concat(name, "Partition" <> Integer.to_string(partition))

    children =
      for name <- names,
          do: {Firenest.ReplicatedState.Server, {name, topology, handler, opts}}

    :ets.new(name, [:named_table, :set, read_concurrency: true])
    :ets.insert(name, {:partitions, partitions, List.to_tuple(names)})

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Firenest.ReplicatedState.Server do
  @moduledoc false
  use Firenest.SyncedServer

  alias Firenest.SyncedServer
  alias Firenest.ReplicatedState.Store

  def child_spec({name, topology, handler, opts}) do
    server_opts = [name: name, topology: topology]

    %{
      id: name,
      start: {SyncedServer, :start_link, [__MODULE__, {name, handler, opts}, server_opts]}
    }
  end

  @impl true
  def init({name, handler, opts}) do
    Process.flag(:trap_exit, true)
    store = Store.new(name)
    broadcast_timeout = Keyword.get(opts, :broadcast_timeout, 50)

    {initial_delta, config} = handler.init(opts)

    {:ok,
     %{
       store: store,
       config: config,
       initial_delta: initial_delta,
       handler: handler,
       broadcast_timer: nil,
       broadcast_timeout: broadcast_timeout,
       clock: 0,
       remote_clocks: %{},
       pending_events: []
     }}
  end

  @impl true
  def handshake_data(%{clock: clock}), do: clock

  @impl true
  def handle_call({:put, key, pid, arg}, _from, state) do
    %{store: store} = state

    link(pid)

    unless Store.present?(store, key, pid) do
      case local_put(arg, key, pid, state) do
        {:put, value, delta, state} ->
          store = Store.local_put(store, key, pid, value, delta)
          {:reply, :ok, %{state | store: store}}

        {:delete, value, _delta} ->
          # TODO: this delta has to propagate remotely before delete
          state = local_delete([value], state)
          {:reply, :ok, state}
      end
    else
      {:reply, {:error, :already_present}, state}
    end
  end

  def handle_call({:update, key, pid, arg}, _from, state) do
    %{store: store} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case local_update(arg, key, pid, delta, value, state) do
          {:put, value, delta, state} ->
            store = Store.local_update(store, key, pid, value, delta)
            {:reply, :ok, %{state | store: store}}

          {:delete, value, _delta} ->
            # TODO: this delta has to propagate remotely before delete
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                state = local_delete([value], state)
                {:reply, :ok, %{state | store: store}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                state = local_delete([value], state)
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                {:reply, :ok, %{state | store: store}}
            end
        end

      :error ->
        {:reply, {:error, :not_present}, state}
    end
  end

  def handle_call({:delete, key, pid}, _from, state) do
    %{store: store} = state

    case Store.local_delete(store, key, pid) do
      {:ok, value, store} ->
        # state = schedule_broadcast_events(state, [{:leave, key, pid}])
        state = local_delete([value], state)
        {:reply, :ok, %{state | store: store}}

      {:last_member, value, store} ->
        unlink_flush(pid)
        state = local_delete([value], state)
        # state = schedule_broadcast_events(state, [{:leave, key, pid}])
        {:reply, :ok, %{state | store: store}}

      {:error, store} ->
        {:reply, {:error, :not_present}, %{state | store: store}}
    end
  end

  def handle_call({:delete, pid}, _from, state) do
    %{store: store} = state

    case Store.local_delete(store, pid) do
      {:ok, leaves, store} ->
        unlink_flush(pid)
        state = local_delete(leaves, state)
        # state = schedule_broadcast_events(state, leaves)
        {:reply, :ok, %{state | store: store}}

      {:error, store} ->
        {:reply, {:error, :not_member}, %{state | store: store}}
    end
  end

  def handle_call({:list, key}, _from, state) do
    %{store: store} = state

    read = fn -> Store.list(store, key) end

    {:reply, read, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    %{store: store} = state

    case Store.local_delete(store, pid) do
      {:ok, leaves, store} ->
        state = local_delete(leaves, state)
        # state = schedule_broadcast_events(state, leaves)
        {:noreply, %{state | store: store}}

      {:error, store} ->
        {:stop, reason, %{state | store: store}}
    end
  end

  def handle_info({:timeout, timer, :broadcast}, %{broadcast_timer: timer} = state) do
    %{pending_events: events, clock: clock} = state
    clock = clock + 1
    SyncedServer.remote_broadcast({:events, clock, events})
    {:noreply, %{state | clock: clock, pending_events: [], broadcast_timer: nil}}
  end

  def handle_info({:update, key, pid, arg}, state) do
    %{store: store} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case local_update(arg, key, pid, delta, value, state) do
          {:put, value, delta, state} ->
            store = Store.local_update(store, key, pid, value, delta)
            {:noreply, %{state | store: store}}

          {:delete, value, _delta} ->
            # TODO: this delta has to propagate remotely before delete
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                state = local_delete([value], state)
                {:noreply, %{state | store: store}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                state = local_delete([value], state)
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                {:noreply, %{state | store: store}}
            end
        end

      :error ->
        # Must have been already deleted, ignore
        {:noreply, state}
    end
  end

  @impl true
  def handle_remote({:catch_up_req, clock}, from, state) do
    {mode, data} = catch_up_reply(state, clock)
    SyncedServer.remote_send(from, {:catch_up, mode, data})
    {:noreply, state}
  end

  def handle_remote({:catch_up, :state_transfer, {clock, transfer}}, from, state) do
    state = handle_state_transfer(state, from, clock, transfer)
    {:noreply, state}
  end

  def handle_remote({:events, remote_clock, events}, from, state) do
    %{remote_clocks: remote_clocks} = state
    local_clock = Map.fetch!(remote_clocks, from)

    if remote_clock == local_clock + 1 do
      remote_clocks = %{remote_clocks | from => remote_clock}
      state = handle_events(state, from, events)
      {:noreply, %{state | remote_clocks: remote_clocks}}
    else
      {:noreply, request_catch_up(state, from, local_clock)}
    end
  end

  @impl true
  def handle_replica({:up, remote_clock}, remote_ref, state) do
    %{remote_clocks: remote_clocks} = state

    case remote_clocks do
      %{^remote_ref => old_clock} when remote_clock > old_clock ->
        # Reconnection, try to catch up
        {:noreply, request_catch_up(state, remote_ref, old_clock)}

      %{^remote_ref => old_clock} ->
        # Reconnection, no remote state change, skip catch up
        # Assert for sanity
        true = old_clock == remote_clock
        {:noreply, state}

      %{} when remote_clock == 0 ->
        # New node, no state, don't catch up
        state = %{state | remote_clocks: Map.put(remote_clocks, remote_ref, 0)}
        {:noreply, state}

      %{} ->
        # New node, catch up
        state = %{state | remote_clocks: Map.put(remote_clocks, remote_ref, 0)}
        {:noreply, request_catch_up(state, remote_ref, 0)}
    end
  end

  def handle_replica(:down, remote_ref, state) do
    %{values: values, remote_clocks: remote_clocks} = state
    delete_ms = [{{:_, remote_ref, :_}, [], [true]}]
    :ets.select_delete(values, delete_ms)
    {:noreply, %{state | remote_clocks: Map.delete(remote_clocks, remote_ref)}}
  end

  defp local_put(arg, key, pid, state) do
    %{handler: handler, config: config, initial_delta: delta} = state

    case handler.local_put(arg, delta, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta}

      {delta, value, {:update_after, update, time}} ->
        Process.send_after(self(), {:update, key, pid, update}, time)
        {:put, value, delta, state}
    end
  end

  defp local_update(arg, key, pid, local_delta, value, state) do
    %{handler: handler, config: config} = state

    case handler.local_update(arg, local_delta, value, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta}

      {delta, value, {:update_after, update, time}} ->
        Process.send_after(self(), {:update, key, pid, update}, time)
        {:put, value, delta, state}
    end
  end

  defp local_delete(leaves, state) do
    %{handler: handler, config: config} = state

    Enum.each(leaves, &handler.local_delete(&1, config))
    state
  end

  defp link(:partition), do: true
  defp link(pid), do: Process.link(pid)

  defp unlink_flush(:partition), do: true

  defp unlink_flush(pid) do
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, _} -> true
    after
      0 -> true
    end
  end

  defp schedule_broadcast_events(%{broadcast_timer: nil} = state, new_events) do
    %{broadcast_timeout: timeout, pending_events: events} = state
    timer = :erlang.start_timer(timeout, self(), :broadcast)
    %{state | broadcast_timer: timer, pending_events: new_events ++ events}
  end

  defp schedule_broadcast_events(%{} = state, new_events) do
    %{pending_events: events} = state
    %{state | pending_events: new_events ++ events}
  end

  defp request_catch_up(state, remote_ref, clock) do
    SyncedServer.remote_send(remote_ref, {:catch_up_req, clock})
    state
  end

  defp handle_events(%{values: values} = state, from, events) do
    {joins, leaves} =
      Enum.reduce(events, {[], []}, fn
        {:leave, key, pid}, {joins, leaves} ->
          leave = {{{key, pid}, from, :_}, [], [true]}
          {joins, [leave | leaves]}

        {:replace, key, pid, value}, {joins, leaves} ->
          join = {{key, pid}, from, value}
          {[join | joins], leaves}

        {:join, key, pid, value}, {joins, leaves} ->
          join = {{key, pid}, from, value}
          {[join | joins], leaves}
      end)

    :ets.insert(values, joins)
    :ets.select_delete(values, leaves)
    state
  end

  # TODO: detect leaves
  # Is there a better way than to clean up and re-insert?
  # This can be problematic for dirty reads!
  defp handle_state_transfer(%{values: values} = state, from, clock, transfer) do
    %{remote_clocks: remote_clocks} = state
    delete_ms = [{{:_, from, :_}, [], [true]}]
    inserts = for {ets_key, value} <- transfer, do: {ets_key, from, value}
    :ets.select_delete(values, delete_ms)
    :ets.insert(values, inserts)
    %{state | remote_clocks: %{remote_clocks | from => clock}}
  end

  # TODO: handle catch-up with events
  defp catch_up_reply(%{values: values}, clock) do
    local_ms = [{{:"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}]
    {:state_transfer, {clock, :ets.select(values, local_ms)}}
  end
end
