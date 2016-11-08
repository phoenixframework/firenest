defmodule Firenest.Topology.Erlang do
  @moduledoc """
  An implementation of Firenest.Topology that relies on the
  Erlang Distribution to connect and exchange messages between
  nodes.

  ## Discovery

  By default, the Erlang distribution requires nodes to be
  connected manually. For example, assuming you start two nodes
  with the same cookie and can reach other in the network:

      nodea> iex --name "nodea@1.2.3.4" --cookie "secret"
      nodeb> iex --name "nodeb@5.6.7.8" --cookie "secret"

  You can manually connect both nodes by calling
  `Node.connect(:"nodea@1.2.3.4")` from node B or by calling
  `Node.connect(:"nodeb@5.6.7.8")` from node A. Topologies also
  provide `connect/2` and `disconnect/2` functions which, for this
  particular topology, is the same as calling the `Node` functions
  above.

  Projects like [libcluster](https://github.com/bitwalker/libcluster)
  are able to automate and manage the connection between nodes by
  doing UDP multicasts, by relying on orchestration tools such as
  Kubernetes, or other. It is recommended choice for those who do
  not want to manually manage their own list of nodes.
  """

  use Supervisor
  @behaviour Firenest.Topology

  def start_link(topology, opts) do
    Supervisor.start_link(__MODULE__, {topology, opts}, name: topology)
  end

  ## Topology callbacks

  # TODO: We need to subscribe to node up events from discovery
  def connect(_topology, node) when is_atom(node) do
    :net_kernel.connect(node)
  end

  # TODO: We need to subscribe to node down events from discovery
  def disconnect(topology, node) when is_atom(node) do
    :net_kernel.disconnect(node)
  end

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

  use GenServer

  def start_link(topology, discovery) do
    GenServer.start_link(__MODULE__, {topology, discovery}, name: discovery)
  end

  ## Callbacks

  def init({topology, discovery}) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)
    update_topology(topology, %{})

    id = id()
    Enum.each(Node.list(), &ping(&1, discovery, id))
    {:ok, %{topology: topology, discovery: discovery, nodes: %{}, id: id}}
  end

  def handle_info({:nodeup, node, _}, %{discovery: discovery, id: id} = state) do
    ping(node, discovery, id)
    {:noreply, state}
  end

  def handle_info({:nodedown, _, _}, state) do
    {:noreply, state}
  end

  def handle_info({:ping, other_id, pid}, %{id: id} = state) do
    pong(pid, id)
    {:noreply, add_node(state, other_id, pid)}
  end

  def handle_info({:pong, other_id, pid}, state) do
    {:noreply, add_node(state, other_id, pid)}
  end

  def handle_info({:DOWN, ref, _, pid, _}, state) when node(pid) != node() do
    {:noreply, delete_node(state, pid, ref)}
  end

  defp ping(node, discovery, id) do
    send({discovery, node}, {:ping, id, self()})
  end

  defp pong(pid, id) do
    send(pid, {:pong, id, self()})
  end

  ## Helpers

  defp id() do
    {:crypto.strong_rand_bytes(12), System.system_time()}
  end

  defp add_node(%{nodes: nodes} = state, id, pid) do
    node = node(pid)
    case nodes do
      %{^node => {^id, _}} ->
        state
      %{^node => _} ->
        state
        |> delete_node_and_notify(node)
        |> add_node_and_notify(node, id, pid)
      %{} ->
        add_node_and_notify(state, node, id, pid)
    end
  end

  defp add_node_and_notify(%{nodes: nodes, topology: topology} = state, node, id, pid) do
    ref = Process.monitor(pid)
    nodes = Map.put(nodes, node, {id, ref})
    update_topology(topology, nodes)
    # TODO: Deliver notification
    %{state | nodes: nodes}
  end

  defp delete_node(%{nodes: nodes} = state, pid, ref) do
    node = node(pid)
    case nodes do
      %{^node => {_, ^ref}} ->
        delete_node_and_notify(state, node)
      %{} ->
        state
    end
  end

  defp delete_node_and_notify(%{nodes: nodes, topology: topology} = state, node) do
    nodes = Map.delete(nodes, node)
    update_topology(topology, nodes)
    # TODO: Deliver notification
    %{state | nodes: nodes}
  end

  defp update_topology(topology, nodes) do
    true = :ets.insert(topology, {:nodes, Map.keys(nodes)})
  end
end
