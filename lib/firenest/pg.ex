defmodule Firenest.PG do
  alias Firenest.SyncedServer

  @type pg() :: SyncedServer.server()

  defdelegate child_spec(opts), to: Firenest.PG.Supervisor

  def join(pg, group, key, pid, meta) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:join, group, key, pid, meta})
  end

  def leave(pg, group, key, pid) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:leave, group, key, pid})
  end

  def leave(pg, pid) when node(pid) == node() do
    servers = partition_infos!(pg)
    replies = multicall(servers, {:leave, pid}, 5_000)

    if :ok in replies do
      :ok
    else
      {:error, :not_member}
    end
  end

  def update(pg, group, key, pid, update) when node(pid) == node() and is_function(update, 1) do
    server = partition_info!(pg, group)
    GenServer.call(server, {:update, group, key, pid, update})
  end

  def replace(pg, group, key, pid, meta) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:replace, group, key, pid, meta})
  end

  def members(pg, group) do
    server = partition_info!(pg, group)
    GenServer.call(server, {:members, group}).()
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
  def init({name, _opts}) do
    Process.flag(:trap_exit, true)
    values = :ets.new(name, [:named_table, :protected, :ordered_set])
    pids = :ets.new(__MODULE__.Pids, [:duplicate_bag, keypos: 2])
    {:ok, %{values: values, pids: pids}}
  end

  @impl true
  def handle_call({:join, group, key, pid, meta}, _from, state) do
    %{values: values, pids: pids} = state
    Process.link(pid)
    ets_key = {group, pid, key}

    if :ets.member(values, ets_key) do
      {:reply, {:error, :already_joined}, state}
    else
      :ets.insert(values, {{group, pid, key}, meta})
      :ets.insert(pids, {group, pid, key})
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
        {:reply, :ok, state}
    end
  end

  def handle_call({:update, group, key, pid, update}, _from, state) do
    %{values: values} = state
    ets_key = {group, pid, key}

    case ets_fetch_element(values, ets_key, 2) do
      {:ok, value} ->
        :ets.insert(values, {ets_key, update.(value)})
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:replace, group, key, pid, meta}, _from, state) do
    %{values: values} = state
    ets_key = {group, pid, key}

    if :ets.member(values, ets_key) do
      :ets.insert(values, {ets_key, meta})
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:leave, pid}, _from, state) do
    %{values: values, pids: pids} = state

    if untrack_pid(pids, values, pid) do
      Process.unlink(pid)
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_member}, state}
    end
  end

  def handle_call({:members, group}, _from, state) do
    %{values: values} = state

    read = fn ->
      ms = [{{{group, :_, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}]
      :ets.select(values, ms)
    end

    {:reply, read, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    %{values: values, pids: pids} = state

    if untrack_pid(pids, values, pid) do
      {:noreply, state}
    else
      {:stop, reason, state}
    end
  end

  defp untrack_pid(pids, values, pid) do
    case :ets.take(pids, pid) do
      [] ->
        false

      list ->
        ms = for key <- list, do: {{key, :_}, [], [true]}
        :ets.select_delete(values, ms)
        true
    end
  end

  defp ets_fetch_element(table, key, pos) do
    {:ok, :ets.lookup_element(table, key, pos)}
  catch
    :error, :badarg -> :error
  end
end
