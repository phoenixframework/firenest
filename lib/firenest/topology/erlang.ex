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

  defmodule Supervisor do
    @moduledoc false

    use Elixir.Supervisor

    def start_link(topology, opts) do
      name = Module.concat(topology, "Supervisor")
      Elixir.Supervisor.start_link(__MODULE__, {topology, opts}, name: name)
    end

    def init({topology, _opts}) do
      ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])
      true = :ets.insert(topology, [adapter: Firenest.Topology.Erlang])

      children = [
        worker(GenServer, [Firenest.Topology.Erlang, topology, [name: topology]])
      ]

      supervise(children, strategy: :one_for_one)
    end
  end

  ## Topology callbacks

  @behaviour Firenest.Topology
  @timeout 5000

  defdelegate start_link(topology, opts), to: Supervisor

  def subscribe(topology, pid) when is_pid(pid) do
    GenServer.call(topology, {:subscribe, pid})
  end

  def unsubscribe(topology, ref) when is_reference(ref) do
    GenServer.call(topology, {:unsubscribe, ref})
  end

  def connect(topology, node) when is_atom(node) do
    fn ->
      subscribe(topology, self())
      case :net_kernel.connect(node) do
        true -> node in nodes(topology) or wait_until({:nodeup, node})
        false -> false
        :ignored -> :ignored
      end
    end
    |> Task.async()
    |> Task.await(:infinity)
  end

  def disconnect(topology, node) when is_atom(node) do
    fn ->
      subscribe(topology, self())
      case node in nodes(topology) and :net_kernel.disconnect(node) do
        true -> wait_until({:nodedown, node})
        false -> false
        :ignored -> :ignored
      end
    end
    |> Task.async()
    |> Task.await(:infinity)
  end

  defp wait_until(msg) do
    receive do
      ^msg -> true
    after
      @timeout -> false
    end
  end

  def broadcast(topology, name, message) when is_atom(name) do
    topology |> nodes() |> Enum.each(&send({name, &1}, message))
  end

  def send(_topology, node, name, message) when is_atom(node) and is_atom(name) do
    send({name, node}, message)
  end

  def node(_topology) do
    Kernel.node()
  end

  def nodes(topology) do
    :ets.lookup_element(topology, :nodes, 2)
  end

  ## GenServer callbacks

  use GenServer

  def init(topology) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)
    update_topology(topology, %{})

    id = id()
    Enum.each(Node.list(), &ping(&1, topology, id))
    {:ok, %{topology: topology, nodes: %{}, id: id, subscribers: %{}}}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    state = put_in(state.subscribers[ref], pid)
    {:reply, ref, state}
  end

  def handle_call({:unsubscribe, ref}, _from, state) do
    {_, state} = pop_in(state.subscribers[ref])
    {:noreply, :ok, state}
  end

  @doc false
  def handle_info({:nodeup, node, _}, %{topology: topology, id: id} = state) do
    ping(node, topology, id)
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

  def handle_info({:DOWN, ref, _, pid, _}, state) when Kernel.node(pid) != Kernel.node() do
    {:noreply, delete_node(state, pid, ref)}
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    {_, state} = pop_in(state.subscribers[ref])
    {:noreply, state}
  end

  defp ping(node, topology, id) do
    send({topology, node}, {:ping, id, self()})
  end

  defp pong(pid, id) do
    send(pid, {:pong, id, self()})
  end

  defp id() do
    {:crypto.strong_rand_bytes(12), System.system_time()}
  end

  defp add_node(%{nodes: nodes} = state, id, pid) do
    node = Kernel.node(pid)
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

  defp add_node_and_notify(%{nodes: nodes, topology: topology, subscribers: subscribers} = state,
                           node, id, pid) do
    ref = Process.monitor(pid)
    nodes = Map.put(nodes, node, {id, ref})
    update_topology(topology, nodes)
    Enum.each(subscribers, fn {_ref, pid} -> send(pid, {:nodeup, node}) end)
    %{state | nodes: nodes}
  end

  defp delete_node(%{nodes: nodes} = state, pid, ref) do
    node = Kernel.node(pid)
    case nodes do
      %{^node => {_, ^ref}} ->
        delete_node_and_notify(state, node)
      %{} ->
        state
    end
  end

  defp delete_node_and_notify(%{nodes: nodes, topology: topology, subscribers: subscribers} = state,
                              node) do
    nodes = Map.delete(nodes, node)
    update_topology(topology, nodes)
    Enum.each(subscribers, fn {_ref, pid} -> send(pid, {:nodedown, node}) end)
    %{state | nodes: nodes}
  end

  defp update_topology(topology, nodes) do
    true = :ets.insert(topology, {:nodes, Map.keys(nodes)})
  end
end
