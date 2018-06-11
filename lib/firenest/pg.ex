defmodule Firenest.PG do
  alias Firenest.SyncedServer

  @type pg() :: atom()
  @type group() :: term()
  @type key() :: term()
  @type value() :: term()

  @doc """

  ## Options

    * `:name` - name for the process, required;
    * `:topology` - name of the supporting topology, required;
    * `:partitions` - number of partitions, defaults to 1;
    * `:broadcast_timeout` - delay of broadcasting local events to other
      nodes, defaults to 50 ms;

  """
  defdelegate child_spec(opts), to: Firenest.PG.Supervisor

  @spec join(pg(), group(), key(), pid(), value()) :: :ok | {:error, :already_joined}
  def join(pg, group, key, pid, value) when node(pid) == node() do
    server = partition_info!(pg, group)
    SyncedServer.call(server, {:join, group, key, pid, value})
  end

  @spec leave(pg(), group(), key(), pid()) :: :ok | {:error, :not_member}
  def leave(pg, group, key, pid) when node(pid) == node() do
    server = partition_info!(pg, group)
    SyncedServer.call(server, {:leave, group, key, pid})
  end

  @spec leave(pg(), pid()) :: :ok | {:error, :not_member}
  def leave(pg, pid) when node(pid) == node() do
    servers = partition_infos!(pg)
    replies = multicall(servers, {:leave, pid}, 5_000)

    if :ok in replies do
      :ok
    else
      {:error, :not_member}
    end
  end

  @spec update(pg(), group(), key(), pid(), (value() -> value())) :: :ok | {:error, :not_member}
  def update(pg, group, key, pid, update) when node(pid) == node() and is_function(update, 1) do
    server = partition_info!(pg, group)
    SyncedServer.call(server, {:update, group, key, pid, update})
  end

  @spec replace(pg(), group(), key(), pid(), value()) :: :ok | {:error, :not_member}
  def replace(pg, group, key, pid, value) when node(pid) == node() do
    server = partition_info!(pg, group)
    SyncedServer.call(server, {:replace, group, key, pid, value})
  end

  @spec members(pg(), group()) :: [{key(), value()}]
  def members(pg, group) do
    server = partition_info!(pg, group)
    SyncedServer.call(server, {:members, group}).()
  end

  # TODO
  # def dirty_list(pg, group)

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

  defp partition_info!(pg, group) do
    hash = :erlang.phash2(group)
    extract = {:element, {:+, {:rem, {:const, hash}, :"$1"}, 1}, :"$2"}
    ms = [{{:partitions, :"$1", :"$2"}, [], [extract]}]
    [info] = :ets.select(pg, ms)
    info
  catch
    :error, :badarg ->
      raise ArgumentError, "unknown group: #{inspect(pg)}"
  end

  defp partition_infos!(pg) do
    Tuple.to_list(:ets.lookup_element(pg, :partitions, 3))
  catch
    :error, :badarg ->
      raise ArgumentError, "unknown group: #{inspect(pg)}"
  end
end

defmodule Firenest.PG.Supervisor do
  @moduledoc false
  use Supervisor

  def child_spec(opts) do
    partitions = Keyword.get(opts, :partitions, 1)
    name = Keyword.fetch!(opts, :name)
    topology = Keyword.fetch!(opts, :topology)
    supervisor = Module.concat(name, "Supervisor")
    arg = {partitions, name, topology, opts}

    %{
      id: __MODULE__,
      start: {Supervisor, :start_link, [__MODULE__, arg, [name: supervisor]]},
      type: :supervisor
    }
  end

  def init({partitions, name, topology, opts}) do
    names =
      for partition <- 0..(partitions - 1),
          do: Module.concat(name, "Partition" <> Integer.to_string(partition))

    children =
      for name <- names,
          do: {Firenest.PG.Server, {name, topology, opts}}

    :ets.new(name, [:named_table, :set, read_concurrency: true])
    :ets.insert(name, {:partitions, partitions, List.to_tuple(names)})

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Firenest.PG.Server do
  @moduledoc false
  use Firenest.SyncedServer

  alias Firenest.SyncedServer

  def child_spec({name, topology, opts}) do
    server_opts = [name: name, topology: topology]

    %{
      id: name,
      start: {SyncedServer, :start_link, [__MODULE__, {name, opts}, server_opts]}
    }
  end

  @impl true
  def init({name, opts}) do
    Process.flag(:trap_exit, true)
    values = :ets.new(name, [:named_table, :protected, :ordered_set])
    pids = :ets.new(__MODULE__.Pids, [:duplicate_bag, keypos: 2])
    broadcast_timeout = Keyword.get(opts, :broadcast_timeout, 50)

    {:ok,
     %{
       values: values,
       pids: pids,
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
  def handle_call({:join, group, key, pid, value}, _from, state) do
    %{values: values, pids: pids} = state
    Process.link(pid)
    ets_key = {group, pid, key}

    if :ets.member(values, ets_key) do
      {:reply, {:error, :already_joined}, state}
    else
      :ets.insert(values, {{group, pid, key}, value})
      :ets.insert(pids, {group, pid, key})
      state = schedule_broadcast_events(state, [{:join, group, key, pid, value}])
      {:reply, :ok, state}
    end
  end

  def handle_call({:leave, group, key, pid}, _from, state) do
    %{values: values, pids: pids} = state
    key = {group, pid, key}
    ms = [{key, [], [true]}]

    case :ets.select_delete(pids, ms) do
      0 ->
        {:reply, {:error, :not_member}, state}

      1 ->
        unless :ets.member(pids, pid) do
          Process.unlink(pid)
        end

        :ets.delete(values, key)
        state = schedule_broadcast_events(state, [{:leave, group, key, pid}])
        {:reply, :ok, state}
    end
  end

  def handle_call({:update, group, key, pid, update}, _from, state) do
    %{values: values} = state
    ets_key = {group, pid, key}

    case ets_fetch_element(values, ets_key, 2) do
      {:ok, value} ->
        new_value = update.(value)
        :ets.insert(values, {ets_key, new_value})
        state = schedule_broadcast_events(state, [{:replace, group, key, pid, new_value}])
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:replace, group, key, pid, value}, _from, state) do
    %{values: values} = state
    ets_key = {group, pid, key}

    if :ets.member(values, ets_key) do
      :ets.insert(values, {ets_key, value})
      state = schedule_broadcast_events(state, [{:replace, group, key, pid, value}])
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:leave, pid}, _from, state) do
    %{values: values, pids: pids} = state

    case untrack_pid(pids, values, pid) do
      {:ok, leaves} ->
        unlink_flush(pid)
        state = schedule_broadcast_events(state, leaves)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:members, group}, _from, state) do
    %{values: values} = state

    read = fn ->
      local = {{{group, :_, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}
      remote = {{{group, :_, :"$1"}, :_, :"$2"}, [], [{{:"$1", :"$2"}}]}
      :ets.select(values, [local, remote])
    end

    {:reply, read, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    %{values: values, pids: pids} = state

    case untrack_pid(pids, values, pid) do
      {:ok, leaves} ->
        state = schedule_broadcast_events(state, leaves)
        {:noreply, state}

      :error ->
        {:stop, reason, state}
    end
  end

  def handle_info({:timeout, timer, :broadcast}, %{broadcast_timer: timer} = state) do
    %{pending_events: events, clock: clock} = state
    clock = clock + 1
    SyncedServer.remote_broadcast({:events, clock, events})
    {:noreply, %{state | clock: clock, pending_events: [], broadcast_timer: nil}}
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

  # TODO: remove data from that node
  def handle_replica(:down, remote_ref, state) do
    %{values: values, remote_clocks: remote_clocks} = state
    delete_ms = [{{:_, remote_ref, :_}, [], [true]}]
    :ets.select_delete(values, delete_ms)
    {:noreply, %{state | remote_clocks: Map.delete(remote_clocks, remote_ref)}}
  end

  defp untrack_pid(pids, values, pid) do
    case :ets.take(pids, pid) do
      [] ->
        :error

      list ->
        ms = for key <- list, do: {{key, :_}, [], [true]}
        :ets.select_delete(values, ms)
        leaves = for {group, pid, key} <- list, do: {:leave, group, key, pid}
        {:ok, leaves}
    end
  end

  defp ets_fetch_element(table, key, pos) do
    {:ok, :ets.lookup_element(table, key, pos)}
  catch
    :error, :badarg -> :error
  end

  defp unlink_flush(pid) do
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
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
        {:leave, group, key, pid}, {joins, leaves} ->
          leave = {{{group, pid, key}, from, :_}, [], [true]}
          {joins, [leave | leaves]}

        {:replace, group, key, pid, value}, {joins, leaves} ->
          join = {{group, pid, key}, from, value}
          {[join | joins], leaves}

        {:join, group, key, pid, value}, {joins, leaves} ->
          join = {{group, pid, key}, from, value}
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
    inserts = for {key, value} <- transfer, do: {key, from, value}
    :ets.select_delete(values, delete_ms)
    :ets.insert(values, inserts)
    state
    %{state | remote_clocks: %{remote_clocks | from => clock}}
  end

  # TODO: handle catch-up with events
  defp catch_up_reply(%{values: values}, _clock) do
    local_ms = [{{:"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}]
    {:state_transfer, :ets.select(values, local_ms)}
  end
end
