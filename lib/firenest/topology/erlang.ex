defmodule Firenest.Topology.Erlang do
  @moduledoc """
  An implementation of Firenest.Topology that uses the
  Erlang Distribution to build a fully meshed topology.
  """

  use Supervisor
  @behaviour Firenest.Topology

  def start_link(topology, opts) do
    Supervisor.start_link(__MODULE__, {topology, opts}, name: topology)
  end

  ## Topology callbacks

  def broadcast(topology, name, message) when is_atom(name) do
    topology |> nodes() |> Enum.each(&send({name, &1}, message))
  end

  def node(_topology) do
    node()
  end

  def nodes(topology) do
    :ets.lookup_element(topology, :nodes, 2)
  end

  ## Supervisor callbacks

  @doc false
  def init({topology, _opts}) do
    ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(topology, [adapter: __MODULE__])

    children = [
      worker(Firenest.Topology.Erlang.Discovery, [topology, Module.concat(topology, "Discovery")])
    ]

    supervise(children, strategy: :one_for_one)
  end
end

defmodule Firenest.Topology.Erlang.Discovery do
  @moduledoc false

  def start_link(topology, discovery) do
    GenServer.start_link(__MODULE__, {topology, discovery}, name: discovery)
  end

  def init({topology, discovery}) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)
    nodes = update_topology(topology, [])
    Enum.each(Node.list(), &ping(&1, discovery))
    {:ok, %{topology: topology, discovery: discovery, nodes: nodes}}
  end

  def handle_info({:nodeup, node, _}, %{discovery: discovery} = state) do
    ping(node, discovery)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _}, state) do
    {:noreply, delete_node(state, node)}
  end

  def handle_info({:ping, node}, %{discovery: discovery} = state) do
    pong(node, discovery)
    {:noreply, add_node(state, node)}
  end

  def handle_info({:pong, node}, state) do
    {:noreply, add_node(state, node)}
  end

  defp add_node(%{nodes: nodes, topology: topology} = state, node) do
    nodes = :ordsets.add_element(node, nodes)
    %{state | nodes: update_topology(topology, nodes)}
  end

  defp delete_node(%{nodes: nodes, topology: topology} = state, node) do
    nodes = :ordsets.del_element(node, nodes)
    %{state | nodes: update_topology(topology, nodes)}
  end

  defp update_topology(topology, nodes) do
    true = :ets.insert(topology, {:nodes, nodes})
    nodes
  end

  defp ping(node, discovery) do
    send({discovery, node}, {:ping, node()})
  end

  defp pong(node, discovery) do
    send({discovery, node}, {:pong, node()})
  end
end