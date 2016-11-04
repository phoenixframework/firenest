defmodule Firenest.Topology.PG2 do
  use Supervisor

  def start_link(topology, opts) do
    Supervisor.start_link(__MODULE__, {topology, opts}, name: topology)
  end

  ## Topology callbacks

  def broadcast(topology, name, message) when is_atom(name) do
    for node <- nodes(topology) do
      send({name, node}, message)
    end
    :ok
  end

  def node(_topology) do
    Kernel.node()
  end

  def nodes(topology) do
    pg2_key(topology)
    |> :pg2.get_members()
    |> Enum.map(&Kernel.node/1)
    |> List.delete(Kernel.node())
  end

  ## Supervisor callbacks

  def init({topology, _opts}) do
    # Setup the ETS table used by topology lookups
    ^topology = :ets.new(topology, [:set, :named_table, read_concurrency: true])
    true = :ets.insert(topology, [{:adapter, __MODULE__}])

    # Setup a PG2 group with the topology and registers the supervisor
    pg2_key = pg2_key(topology)
    :ok = :pg2.create(pg2_key)
    :ok = :pg2.join(pg2_key, self())

    supervise([], strategy: :one_for_one)
  end

  @compile {:inline, pg2_key: 1}
  defp pg2_key(topology), do: {:firenest, topology}
end
