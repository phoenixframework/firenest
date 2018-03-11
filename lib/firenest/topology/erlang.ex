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
  provide `connect/2` and `disconnect/2` functions which, besides
  calling the `Node` functions above, also checks if the topology
  process itself is up and running.

  Projects like [libcluster](https://github.com/bitwalker/libcluster)
  are able to automate and manage the connection between nodes by
  doing UDP multicasts, by relying on orchestration tools such as
  Kubernetes, or other. It is recommended choice for those who do
  not want to manually manage their own list of nodes.
  """

  defmodule Supervisor do
    @moduledoc false

    use Elixir.Supervisor

    def start_link(opts) do
      topology = opts[:name]
      name = Module.concat(topology, "Supervisor")
      Elixir.Supervisor.start_link(__MODULE__, {topology, opts}, name: name)
    end

    def init({topology, _opts}) do
      ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])
      true = :ets.insert(topology, {:adapter, Firenest.Topology.Erlang})

      children = [
        %{
          id: topology,
          start: {GenServer, :start_link, [Firenest.Topology.Erlang, topology, [name: topology]]}
        }
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end

  ## Topology callbacks

  @behaviour Firenest.Topology
  @timeout 5000

  defdelegate start_link(opts), to: Supervisor

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
    topology |> nodes() |> Enum.each(&Process.send({name, &1}, message, [:noconnect]))
  end

  def send(topology, node, name, message) when is_atom(node) and is_atom(name) do
    if node == Kernel.node() or node in nodes(topology) do
      Process.send({name, node}, message, [:noconnect])
      :ok
    else
      {:error, :noconnection}
    end
  end

  def sync_named(topology, pid, timeout \\ 5000) when is_pid(pid) do
    case Process.info(pid) do
      {:registered_name, []} ->
        raise ArgumentError,
              "cannot sync process #{inspect(pid)} because it hasn't been registered"

      {:registered_name, name} ->
        GenServer.call(topology, {:sync_named, pid, name}, timeout)
    end
  end

  def node(_topology) do
    Kernel.node()
  end

  def nodes(topology) do
    :ets.lookup_element(topology, :nodes, 2)
  end

  # Subscribing to the topology events is private right now,
  # we can make it public if necessary but sync_named/3 should
  # be enough for all purposes.
  defp subscribe(topology, pid) when is_pid(pid) do
    GenServer.call(topology, {:subscribe, pid})
  end

  ## GenServer callbacks

  use GenServer

  def init(topology) do
    # We need to monitor nodes before we do the first broadcast.
    # Otherwise a node can come up between the first broadcast and
    # the first notification.
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)

    # Node names are also stored in an ETS table for fast lookups.
    persist_node_names(topology, %{})

    # We generate a unique ID to be used alongside the node name
    # to guarantee uniqueness in case of restarts. Then we do a
    # broadcast over the Erlang topology to find other processes
    # like ours. The other server monitor states will be carried
    # in their pongs.
    id = id()
    Enum.each(Node.list(), &ping(&1, topology, id, []))

    state = %{
      topology: topology,
      nodes: %{},
      id: id,
      monitors: %{},
      local_names: %{},
      subscribers: %{}
    }

    {:ok, state}
  end

  ## Local messages

  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    state = put_in(state.subscribers[ref], pid)
    {:reply, ref, state}
  end

  # Receives the sync monitor command from a local process and broadcast
  # this monitor is up in all known instances of this topology.
  def handle_call({:sync_named, pid, name}, _from, state) do
    %{topology: topology, id: id, nodes: nodes} = state

    case maybe_remove_dead_monitor(state, name) do
      {:ok, state} ->
        ref = Process.monitor(name)
        state = put_in(state.monitors[ref], name)
        state = put_in(state.local_names[name], {pid, ref})

        nodes =
          for {node, {id, _, remote_names}} <- nodes,
              Map.has_key?(remote_names, name),
              do: {node, id}

        topology_broadcast(topology, {:monitor_up, Kernel.node(), id, name, ref})
        {:reply, {:ok, nodes}, state}

      {:error, existing_pid} ->
        {:reply, {:error, {:already_synced, existing_pid}}, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, %{monitors: monitors} = state) do
    case monitors do
      %{^ref => _} ->
        {:noreply, remove_dead_monitor(state, ref)}

      %{} ->
        {_, state} = pop_in(state.subscribers[ref])
        {:noreply, state}
    end
  end

  ## Distributed messages

  # This is the message received from remote nodes when they have a
  # local monitor up.
  def handle_info({:monitor_up, node, id, name, monitor_ref}, state) do
    %{nodes: nodes, local_names: local_names} = state

    state =
      case nodes do
        # We know this node. The other node guarantees to deliver a monitor_down
        # before monitor_up for the same name, so we don't need to check this here.
        %{^node => {^id, node_ref, remote_names}} ->
          local_monitor_up(local_names, node, id, name)
          put_in(state.nodes[node], {id, node_ref, Map.put(remote_names, name, monitor_ref)})

        # We either have a mismatched or an unknown ID because the
        # PONG message has not been processed yet.
        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:monitor_down, node, id, name, monitor_ref}, state) do
    %{nodes: nodes, local_names: local_names} = state

    state =
      case nodes do
        # We know this node and therefore we must know this name-monitor pair.
        %{^node => {^id, node_ref, remote_names}} ->
          ^monitor_ref = Map.fetch!(remote_names, name)
          local_monitor_down(local_names, node, id, name)
          put_in(state.nodes[node], {id, node_ref, Map.delete(remote_names, name)})

        # We either have a mismatched or an unknown ID because the
        # PONG message has not been processed yet.
        _ ->
          state
      end

    {:noreply, state}
  end

  # This message comes from :net_kernel.monitor_nodes/2. Note it is not
  # guaranteed that we will receive a nodedown before a nodeup with the
  # same name. More info: http://erlang.org/pipermail/erlang-questions/2016-November/090795.html
  def handle_info({:nodeup, node, _}, state) do
    %{topology: topology, id: id, monitors: monitors} = state
    ping(node, topology, id, Map.to_list(monitors))
    {:noreply, state}
  end

  # Sent by :net_kernel.monitor_nodes/2 on node down. We don't worry
  # about it because we already monitor the topology process and the
  # monitor message is guaranteed to be delivered before nodedown.
  def handle_info({:nodedown, _, _}, state) do
    {:noreply, state}
  end

  # If two nodes come up at the same time, ping may be received twice.
  # So we need to make sure to handle two pings/pongs.
  def handle_info({:ping, other_id, pid, monitors}, %{id: id} = state) do
    pong(pid, id, Map.to_list(state.monitors))
    {:noreply, add_node(state, other_id, pid, monitors)}
  end

  def handle_info({:pong, other_id, pid, monitors}, state) do
    {:noreply, add_node(state, other_id, pid, monitors)}
  end

  def handle_info({:DOWN, ref, _, pid, _}, state) when Kernel.node(pid) != Kernel.node() do
    {:noreply, delete_node(state, pid, ref)}
  end

  ## Helpers

  defp ping(node, topology, id, monitors) when is_list(monitors) do
    send({topology, node}, {:ping, id, self(), monitors})
  end

  defp pong(pid, id, monitors) when is_list(monitors) do
    send(pid, {:pong, id, self(), monitors})
  end

  defp id() do
    {:crypto.strong_rand_bytes(4), System.system_time()}
  end

  defp add_node(%{nodes: nodes} = state, id, pid, monitors) do
    node = Kernel.node(pid)
    new_remote_names = for {ref, name} <- monitors, do: {name, ref}, into: %{}

    case nodes do
      %{^node => {^id, ref, old_remote_names}} ->
        :ok = diff_monitors(state, node, id, old_remote_names, monitors)
        put_in(state.nodes[node], {id, ref, new_remote_names})

      %{^node => _} ->
        state
        |> delete_node_and_notify(node)
        |> add_node_and_notify(node, id, pid, new_remote_names, monitors)

      %{} ->
        add_node_and_notify(state, node, id, pid, new_remote_names, monitors)
    end
  end

  defp delete_node(%{nodes: nodes} = state, pid, ref) do
    node = Kernel.node(pid)

    case nodes do
      %{^node => {_, ^ref, _}} -> delete_node_and_notify(state, node)
      %{} -> state
    end
  end

  defp diff_monitors(state, node, id, remote_names, monitors) when is_list(monitors) do
    {added, removed} =
      Enum.reduce(monitors, {[], remote_names}, fn {ref, name}, {added, removed} ->
        case remote_names do
          %{^name => ^ref} ->
            {added, Map.delete(removed, ref)}
          %{} ->
            {[name | added], removed}
        end
      end)

    %{local_names: local_names} = state

    for {name, _ref} <- removed do
      local_monitor_down(local_names, node, id, name)
    end

    for name <- added do
      local_monitor_up(local_names, node, id, name)
    end

    :ok
  end

  defp add_node_and_notify(state, node, id, pid, remote_names, monitors) do
    %{
      topology: topology,
      nodes: nodes,
      local_names: local_names,
      subscribers: subscribers
    } = state

    # Add the node, notify the node, notify the services.
    nodes = Map.put(nodes, node, {id, Process.monitor(pid), remote_names})
    persist_node_names(topology, nodes)

    _ = for {_, pid} <- subscribers, do: send(pid, {:nodeup, node})
    _ = for {_, name} <- monitors, do: local_monitor_up(local_names, node, id, name)
    %{state | nodes: nodes}
  end

  defp delete_node_and_notify(state, node) do
    %{
      topology: topology,
      nodes: nodes,
      local_names: local_names,
      subscribers: subscribers
    } = state

    # Notify the services, remove the node, notify the node.
    {id, _ref, remote_names} = Map.fetch!(nodes, node)
    _ = for {name, _} <- remote_names, do: local_monitor_down(local_names, node, id, name)

    nodes = Map.delete(nodes, node)
    persist_node_names(topology, nodes)

    _ = for {_, pid} <- subscribers, do: send(pid, {:nodedown, node})
    %{state | nodes: nodes}
  end

  defp local_monitor_up(local_names, node, id, name) do
    local_send(local_names, name, {:named_up, node, id, name})
  end

  defp local_monitor_down(local_names, node, id, name) do
    local_send(local_names, name, {:named_down, node, id, name})
  end

  # Sends a message to the process named `name` in the topology.
  # Note that we use the pid instead of sending a message to name
  # to avoid races in case the process dies and we end-up accidentally
  # messaging the new process.
  defp local_send(local_names, name, message) do
    case local_names do
      %{^name => {pid, _}} -> send(pid, message)
      %{} -> :error
    end

    :ok
  end

  defp maybe_remove_dead_monitor(%{local_names: local_names} = state, name) do
    case local_names do
      %{^name => {pid, ref}} ->
        if Process.alive?(pid) do
          {:error, pid}
        else
          Process.demonitor(ref, [:flush])
          {:ok, remove_dead_monitor(state, ref)}
        end

      %{} ->
        {:ok, state}
    end
  end

  defp remove_dead_monitor(%{id: id, topology: topology} = state, ref) do
    {name, state} = pop_in(state.monitors[ref])
    {_, state} = pop_in(state.local_names[name])
    topology_broadcast(topology, {:monitor_down, Kernel.node(), id, name, ref})
    state
  end

  defp persist_node_names(topology, nodes) do
    true = :ets.insert(topology, {:nodes, nodes |> Map.keys |> Enum.sort})
  end

  defp topology_broadcast(topology, message) do
    topology
    |> :ets.lookup_element(:nodes, 2)
    |> Enum.each(&Process.send({topology, &1}, message, [:noconnect]))
  end
end
