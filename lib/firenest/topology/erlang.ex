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

  @behaviour Firenest.Topology
  @timeout 5000

  defdelegate child_spec(opts), to: Firenest.Topology.Erlang.Server

  def connect(topology, node) when is_atom(node) do
    fn ->
      ref = subscribe(topology, self())

      case :net_kernel.connect(node) do
        true -> node in nodes(topology) or wait_until({:nodeup, ref, node})
        false -> false
        :ignored -> :ignored
      end
    end
    |> Task.async()
    |> Task.await(:infinity)
  end

  def disconnect(topology, node) when is_atom(node) do
    fn ->
      ref = subscribe(topology, self())

      case node in nodes(topology) and :net_kernel.disconnect(node) do
        true -> wait_until({:nodedown, ref, node})
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

  def sync_named(topology, pid, timeout \\ @timeout) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
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
end

defmodule Firenest.Topology.Erlang.Server do
  @moduledoc false

  use GenServer
  require Logger

  def start_link(opts) do
    topology = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, topology, name: topology)
  end

  def init(topology) do
    Process.flag(:trap_exit, true)
    # Setup the topology ets table contract.
    ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])

    # Node names are also stored in an ETS table for fast lookups.
    # We need to do this before we write the adapter because once
    # the adapter is written the table is considered as ready.
    persist_node_names(topology, %{})

    true = :ets.insert(topology, {:adapter, Firenest.Topology.Erlang})

    # We need to monitor nodes before we do the first broadcast.
    # Otherwise a node can come up between the first broadcast and
    # the first notification.
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)

    # We generate a unique ID to be used alongside the node name
    # to guarantee uniqueness in case of restarts. Then we do a
    # broadcast over the Erlang topology to find other processes
    # like ours. The other server monitor states will be carried
    # in their pongs.
    state = %{
      clock: 0,
      id: id(),
      monitors: %{},
      nodes: %{},
      local_names: %{},
      subscribers: %{},
      topology: topology
    }

    Enum.each(Node.list(), &ping(state, &1))
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
        Process.link(pid)
        ref = Process.monitor(name)
        state = put_in(state.monitors[ref], name)
        state = put_in(state.local_names[name], {pid, ref})

        nodes =
          for {node, {id, _, _, remote_names}} <- nodes,
              Map.has_key?(remote_names, name),
              do: {node, id}

        {clock, state} = bump_clock(state)
        topology_broadcast(topology, {:monitor_up, Kernel.node(), id, clock, name, ref})
        {:reply, {:ok, nodes}, state}

      {:error, existing_pid} ->
        {:reply, {:error, {:already_synced, existing_pid}}, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    # ignore, we'll receive a DOWN for the process as well
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, pid, _}, state) when Kernel.node(pid) != Kernel.node() do
    {:noreply, delete_node(state, pid, ref)}
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
  def handle_info({:monitor_up, node, id, clock, name, monitor_ref}, state) do
    %{nodes: nodes, local_names: local_names} = state

    state =
      case nodes do
        # We know this node. The other node guarantees to deliver a monitor_down
        # before monitor_up for the same name, so we don't need to check this here.
        %{^node => {^id, old_clock, node_ref, remote_names}} when old_clock == clock - 1 ->
          local_monitor_up(local_names, node, id, name)

          put_in(
            state.nodes[node],
            {id, clock, node_ref, Map.put(remote_names, name, monitor_ref)}
          )

        %{^node => {^id, old_clock, node_ref, remote_names}} ->
          clocks_out_of_sync(state, node, old_clock, clock, node_ref, remote_names)

        # We either have a mismatched or an unknown ID because the
        # PONG message has not been processed yet.
        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:monitor_down, node, id, clock, name, monitor_ref}, state) do
    %{nodes: nodes, local_names: local_names} = state

    state =
      case nodes do
        # We know this node and therefore we must know this name-monitor pair.
        %{^node => {^id, old_clock, node_ref, remote_names}} when old_clock == clock - 1 ->
          ^monitor_ref = Map.fetch!(remote_names, name)
          local_monitor_down(local_names, node, id, name)

          put_in(
            state.nodes[node],
            {id, clock, node_ref, Map.delete(remote_names, name)}
          )

        %{^node => {^id, old_clock, node_ref, remote_names}} ->
          clocks_out_of_sync(state, node, old_clock, clock, node_ref, remote_names)

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
    ping(state, node)
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
  def handle_info({:ping, pid, other_id, clock, monitors}, state) do
    pong(state, pid)
    {:noreply, add_node(state, pid, other_id, clock, monitors)}
  end

  def handle_info({:pong, pid, other_id, clock, monitors}, state) do
    {:noreply, add_node(state, pid, other_id, clock, monitors)}
  end

  ## Helpers

  defp ping(state, node) do
    %{topology: topology, id: id, clock: clock, monitors: monitors} = state
    monitors = Map.to_list(monitors)
    Process.send({topology, node}, {:ping, self(), id, clock, monitors}, [:noconnect])
  end

  defp pong(state, pid) do
    %{id: id, clock: clock, monitors: monitors} = state
    monitors = Map.to_list(monitors)
    Process.send(pid, {:pong, self(), id, clock, monitors}, [:noconnect])
  end

  defp id() do
    {:crypto.strong_rand_bytes(4), System.system_time()}
  end

  defp add_node(%{nodes: nodes} = state, pid, id, clock, monitors) do
    node = Kernel.node(pid)
    new_remote_names = for {ref, name} <- monitors, do: {name, ref}, into: %{}

    case nodes do
      %{^node => {^id, ^clock, _, _}} ->
        state

      %{^node => {^id, _, ref, old_remote_names}} ->
        :ok = diff_monitors(state, node, id, old_remote_names, monitors)
        put_in(state.nodes[node], {id, clock, ref, new_remote_names})

      %{^node => _} ->
        state
        |> delete_node_and_notify(node)
        |> add_node_and_notify(node, pid, id, clock, new_remote_names, monitors)

      %{} ->
        add_node_and_notify(state, node, pid, id, clock, new_remote_names, monitors)
    end
  end

  defp delete_node(%{nodes: nodes} = state, pid, ref) do
    node = Kernel.node(pid)

    case nodes do
      %{^node => {_, _, ^ref, _}} -> delete_node_and_notify(state, node)
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

  defp add_node_and_notify(state, node, pid, id, clock, remote_names, monitors) do
    %{
      topology: topology,
      nodes: nodes,
      local_names: local_names,
      subscribers: subscribers
    } = state

    # Add the node, notify the node, notify the services.
    nodes = Map.put(nodes, node, {id, clock, Process.monitor(pid), remote_names})
    persist_node_names(topology, nodes)

    _ = for {ref, pid} <- subscribers, do: send(pid, {:nodeup, ref, node})
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
    {id, _clock, _ref, remote_names} = Map.fetch!(nodes, node)
    _ = for {name, _} <- remote_names, do: local_monitor_down(local_names, node, id, name)

    nodes = Map.delete(nodes, node)
    persist_node_names(topology, nodes)

    _ = for {ref, pid} <- subscribers, do: send(pid, {:nodedown, ref, node})
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

    {clock, state} = bump_clock(state)
    topology_broadcast(topology, {:monitor_down, Kernel.node(), id, clock, name, ref})

    state
  end

  defp bump_clock(%{clock: clock} = state) do
    clock = clock + 1
    {clock, %{state | clock: clock}}
  end

  defp clocks_out_of_sync(state, _node, :ping, _clock, _node_ref, _remote_names) do
    state
  end

  defp clocks_out_of_sync(state, node, old_clock, clock, node_ref, remote_names) do
    Logger.error(
      "Firenest.Topology.Erlang clock (value #{clock}) from node #{inspect(node)} " <>
        "got out of sync with clock (value #{old_clock}) stored in node " <>
        "#{inspect(Kernel.node())}. A ping message was sent to catch up."
    )

    ping(state, node)
    put_in(state.nodes[node], {node, :ping, node_ref, remote_names})
  end

  defp persist_node_names(topology, nodes) do
    true = :ets.insert(topology, {:nodes, nodes |> Map.keys() |> Enum.sort()})
  end

  defp topology_broadcast(topology, message) do
    topology
    |> :ets.lookup_element(:nodes, 2)
    |> Enum.each(&Process.send({topology, &1}, message, [:noconnect]))
  end
end
