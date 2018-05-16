defmodule Firenest.PG do
  alias Firenest.SyncedServer

  @type pg() :: SyncedServer.server()

  defdelegate child_spec(opts), to: Firenest.PG.Supervisor

  def track(pg, pid, group, key, meta) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:track, pid, group, key, meta})
  end

  def untrack(pg, pid, group, key) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:untrack, pid, group, key})
  end

  def untrack(pg, pid) when node(pid) == node() do
    servers = partition_infos!(pg)
    multicall(servers, {:untrack, pid}, 5_000)
  end

  def update(pg, pid, group, key, meta) when node(pid) == node() do
    server = partition_info!(pg, group)
    GenServer.call(server, {:update, pid, group, key, meta})
  end

  def list(pg, group) do
    server = partition_info!(pg, group)
    GenServer.call(server, {:list, group}).()
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
    |> Enum.each(fn ref ->
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
  def handle_call({:track, pid, group, key, meta}, _from, state) do
    %{values: values, pids: pids} = state
    Process.link(pid)
    :ets.insert(values, {{group, pid, key}, meta})
    :ets.insert(pids, {group, pid, key})
    {:reply, :ok, state}
  end

  def handle_call({:untrack, pid, group, key}, _from, state) do
    %{values: values, pids: pids} = state
    key = {group, pid, key}
    ms = [{key, [], [true]}]

    case :ets.select_delete(pids, ms) do
      0 ->
        {:reply, {:error, :not_tracked}, state}

      1 ->
        unless :ets.member(pids, pid) do
          Process.unlink(pid)
        end

        :ets.delete(values, key)
        {:reply, :ok, state}
    end
  end

  def handle_call({:untrack, pid}, _from, state) do
    %{values: values, pids: pids} = state

    if untrack_pid(pids, values, pid) do
      Process.unlink(pid)
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_tracked}, state}
    end
  end

  def handle_call({:list, group}, _from, state) do
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
end
