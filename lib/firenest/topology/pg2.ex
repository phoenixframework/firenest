defmodule Firenest.Topology.PG2 do
  @moduledoc """
  An implementation of Firenest.Topology that uses the
  Erlang Distribution and PG2.

  This topology only uses PG2 to detect which nodes are
  running instances of this topology. Messages, broadcasts
  and failure handling are all implemented by relying on
  the Erlang Distribution.
  """

  use Supervisor
  @behaviour Firenest.Topology

  def start_link(topology, opts) do
    Supervisor.start_link(__MODULE__, {topology, opts}, name: topology)
  end

  ## Topology callbacks

  def broadcast(topology, name, message) when is_atom(name) do
    case get_nodes(topology) do
      {:ok, nodes} ->
        Enum.each(nodes, &send({name, &1}, message))
      {:error, _} = error ->
        error
    end
  end

  def node(_topology) do
    Kernel.node()
  end

  def nodes(topology) do
    case get_nodes(topology) do
      {:ok, nodes} -> nodes
      {:error, _} -> []
    end
  end

  defp get_nodes(topology) do
    case :pg2.get_members(pg2_key(topology)) do
      pids when is_list(pids) ->
        {:ok, pids |> Enum.map(&Kernel.node/1) |> List.delete(Kernel.node())}
      {:error, _} = error ->
        error
    end
  end

  ## Supervisor callbacks

  @doc false
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
