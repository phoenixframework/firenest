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
