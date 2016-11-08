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
