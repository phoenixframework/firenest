alias Firenest.Topology

defmodule Firenest.Monitor do
  @moduledoc """
  A monitoring service on top of a topology.

  This is typically started by the topology to provide the
  `Firenest.Topology.monitor/3` functionality.
  """

  import Supervisor.Spec

  @doc """
  Starts the monitoring service on `topology` with name `monitor`.

  It must be started as a supervisor under the same tree as the topology.
  """
  def start_link(topology, monitor) do
    local = monitor
    remote = Module.concat(monitor, "Remote")
    supervisor = Module.concat(monitor, "Supervisor")

    children = [
      worker(Firenest.Monitor.Local, [topology, local, remote]),
      worker(Firenest.Monitor.Remote, [topology, local, remote])
    ]

    Supervisor.start_link(children, name: supervisor, strategy: :one_for_all)
  end

  @doc """
  Invokes the monitoring service to monitor the given `name` in `node`.
  """
  def monitor(monitor, node, name) when is_atom(node) and is_atom(name) do
    GenServer.call(monitor, {:monitor, name, node})
  end
end

defmodule Firenest.Monitor.Local do
  # The local process that sends requests to the remote one.
  @moduledoc false

  use GenServer

  def start_link(topology, local, remote) do
    GenServer.start_link(__MODULE__, {topology, remote}, name: local)
  end

  # Callbacks

  def init({topology, remote}) do
    Process.flag(:trap_exit, true)
    Topology.subscribe(topology, self())
    {:ok, %{topology: topology, remote: remote, refs: %{}, nodes: %{}}}
  end

  def handle_call(
        {:monitor, name, node},
        {pid, _},
        %{remote: remote, topology: topology, refs: refs, nodes: nodes} = state
      ) do
    Process.link(pid)
    ref = Process.monitor(pid)
    message = {:monitor, name, Topology.node(topology), ref}

    case Topology.send(topology, node, remote, message) do
      :ok ->
        refs = Map.put(refs, ref, {pid, name, node})
        nodes = Map.update(nodes, node, [ref], &[ref | &1])
        {:reply, ref, %{state | refs: refs, nodes: nodes}}

      {:error, _} ->
        send_down(pid, ref, name, node, :noconnection)
        {:reply, ref, state}
    end
  end

  def handle_info({:nodeup, _}, state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, node}, %{nodes: nodes, refs: refs} = state) do
    {entries, nodes} = Map.pop(nodes, node, [])

    refs =
      Enum.reduce(entries, refs, fn ref, acc ->
        {{pid, name, ^node}, acc} = Map.pop(acc, ref)
        send_down(pid, ref, name, node, :noconnection)
        acc
      end)

    {:noreply, %{state | refs: refs, nodes: nodes}}
  end

  def handle_info({:EXIT, _, _}, state) do
    {:noreply, state}
  end

  def handle_info({:down, ref, reason}, state) do
    case pop_in(state.refs[ref]) do
      {{pid, name, node}, state} ->
        state = delete_node_ref(state, node, ref)
        send_down(pid, ref, name, node, reason)
        {:noreply, state}

      {nil, state} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, %{topology: topology, remote: remote} = state) do
    {{_pid, _name, node}, state} = pop_in(state.refs[ref])
    Topology.send(topology, node, remote, {:demonitor, Topology.node(topology), ref})
    {:noreply, delete_node_ref(state, node, ref)}
  end

  defp send_down(pid, ref, name, node, reason) do
    Process.demonitor(ref, [:flush])
    send(pid, {:DOWN, ref, :process, {name, node}, reason})
  end

  defp delete_node_ref(state, node, ref) do
    update_in(state.nodes[node], &(&1 && List.delete(&1, ref)))
  end
end

defmodule Firenest.Monitor.Remote do
  # The remote process that receives requests from other nodes.
  @moduledoc false

  use GenServer

  def start_link(topology, local, remote) do
    GenServer.start_link(__MODULE__, {topology, local}, name: remote)
  end

  # Callbacks

  def init({topology, local}) do
    Process.link(Process.whereis(topology))
    Topology.subscribe(topology, self())
    {:ok, %{topology: topology, local: local, refs: %{}, nodes: %{}}}
  end

  def handle_info({:nodeup, _}, state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, node}, %{nodes: nodes, refs: refs} = state) do
    {entries, nodes} = Map.pop(nodes, node, [])

    refs =
      Enum.reduce(entries, refs, fn ref, acc ->
        {{^node, _}, acc} = Map.pop(acc, ref)
        Process.demonitor(ref, [:flush])
        acc
      end)

    {:noreply, %{state | refs: refs, nodes: nodes}}
  end

  def handle_info({:monitor, name, node, remote_ref}, %{refs: refs, nodes: nodes} = state) do
    ref = Process.monitor(name)
    refs = Map.put(refs, ref, {node, remote_ref})
    refs = Map.put(refs, {node, remote_ref}, ref)
    nodes = Map.update(nodes, node, [ref], &[ref | &1])
    {:noreply, %{state | refs: refs, nodes: nodes}}
  end

  def handle_info({:demonitor, node, remote_ref}, state) do
    case pop_in(state.refs[{node, remote_ref}]) do
      {ref, state} when is_reference(ref) ->
        Process.demonitor(ref, [:flush])
        {{node, ^remote_ref}, state} = pop_in(state.refs[ref])
        {:noreply, delete_node_ref(state, node, ref)}

      {nil, state} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{topology: topology, local: local} = state) do
    {{node, remote_ref}, state} = pop_in(state.refs[ref])
    {^ref, state} = pop_in(state.refs[{node, remote_ref}])
    Topology.send(topology, node, local, {:down, remote_ref, reason})
    {:noreply, delete_node_ref(state, node, ref)}
  end

  defp delete_node_ref(state, node, ref) do
    update_in(state.nodes[node], &(&1 && List.delete(&1, ref)))
  end
end
