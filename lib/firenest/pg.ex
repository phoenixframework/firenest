defmodule Firenest.PG do
  alias Firenest.SyncedServer

  @type pg() :: SyncedServer.server()

  defdelegate child_spec(opts), to: Firenest.PG.Supervisor

  def track(pg, pid, topic, key, meta) when node(pid) == node() do
    server = partition_info!(pg, topic)
    GenServer.call(server, {:track, pid, topic, key, meta})
  end

  def untrack(pg, pid, topic, key) when node(pid) == node() do
    server = partition_info!(pg, topic)
    GenServer.call(server, {:untrack, pid, topic, key})
  end

  def untrack(pg, pid) when node(pid) == node() do
    servers = partition_infos!(pg)
    multicall(servers, {:untrack, pid}, 5_000)
  end

  def update(pg, pid, topic, key, meta) when node(pid) == node() do
    server = partition_info!(pg, topic)
    GenServer.call(server, {:update, pid, topic, key, meta})
  end

  def list(pg, topic) do
    server = partition_info!(pg, topic)
    GenServer.call(server, {:list, topic}).()
  end

  # TODO
  # def dirty_list(pg, topic)

  defp multicall(pids, request, timeout) do
    pids
    |> Enum.map(fn pid ->
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

  defp partition_info!(pg, topic) do
    hash = :erlang.phash2(topic)
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
      start: {Supervisor, :start_link, [__MODULE__, arg, name: supervisor]},
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
    values = :ets.new(name, [:named_table, :protected, :ordered_set])
    pids = :ets.new(__MODULE__.Pids, [:duplicate_bag])
    {:ok, %{values: values, pids: pids}}
  end

  @impl true
  # def handle_call({:track, pid, topic, key, meta}, state) do

  # end

  # def handle_call({:untrack, pid, topic, key}, state) do

  # end

  # def handle_call({:untrack, pid}, state) do

  # end

  # def handle_call({:list, topic}, state) do

  # end
end
