defmodule Firenest.Monitor do
  @moduledoc """
  A monitoring service on top of a topology.

  This is typically started by the topologies themselves.
  """

  use GenServer
  alias Firenest.Topology

  defmacrop incoming(node, ref) do
    quote do: {:incoming, unquote(node), unquote(ref)}
  end

  defmacrop outgoing(pid, name, node) do
    quote do: {:outgoing, unquote(pid), unquote(name), unquote(node)}
  end

  def start_link(topology, monitor) do
    GenServer.start_link(__MODULE__, {topology, monitor}, name: monitor)
  end

  def monitor(monitor, node, name) when is_atom(name) do
    GenServer.call(monitor, {:monitor, name, node})
  end

  ## Callbacks

  def init({topology, monitor}) do
    Process.link(Process.whereis(topology))
    Topology.subscribe(topology, self())
    {:ok, %{topology: topology, monitor: monitor, refs: %{}, outgoing: %{}, incoming: %{}}}
  end

  def handle_call({:monitor, name, node}, {pid, _},
                  %{monitor: monitor, topology: topology, refs: refs, outgoing: outgoing} = state) do
    ref = Process.monitor(pid)
    message = {:remote_monitor, name, Topology.node(topology), ref}

    case Topology.send(topology, node, monitor, message) do
      :ok ->
        refs = Map.put(refs, ref, outgoing(pid, name, node))
        outgoing = Map.update(outgoing, node, [ref], &[ref | &1])
        {:reply, ref, %{state | refs: refs, outgoing: outgoing}}
      {:error, _} ->
        send(pid, {:DOWN, ref, :process, {name, node}, :noconnection})
        {:reply, ref, state}
    end
  end

  def handle_info({:nodeup, _}, state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, node},
                  %{outgoing: outgoing, incoming: incoming, refs: refs} = state) do
    {entries, outgoing} = Map.pop(outgoing, node, [])
    refs =
      Enum.reduce(entries, refs, fn ref, acc ->
        {outgoing(pid, name, ^node), acc} = Map.pop(acc, ref)
        send(pid, {:DOWN, ref, :process, {name, node}, :noconnection})
        acc
      end)

    {entries, incoming} = Map.pop(incoming, node, [])
    refs =
      Enum.reduce(entries, refs, fn ref, acc ->
        {incoming(^node, _), acc} = Map.pop(acc, ref)
        Process.demonitor(ref, [:flush])
        acc
      end)

    {:noreply, %{state | refs: refs, outgoing: outgoing, incoming: incoming}}
  end

  def handle_info({:remote_monitor, name, node, remote_ref},
                  %{refs: refs, incoming: incoming} = state) do
    ref = Process.monitor(name)
    refs = Map.put(refs, ref, incoming(node, remote_ref))
    incoming = Map.update(incoming, node, [ref], &[ref | &1])
    {:noreply, %{state | refs: refs, incoming: incoming}}
  end

  def handle_info({:remote_down, ref, reason}, state) do
    case pop_in(state.refs[ref]) do
      {outgoing(pid, name, node), state} ->
        state = update_in(state.outgoing[node], &(&1 && List.delete(&1, ref)))
        send(pid, {:DOWN, ref, :process, {name, node}, reason})
        {:noreply, state}
      {nil, state} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, reason},
                  %{topology: topology, monitor: monitor} = state) do
    case pop_in(state.refs, ref) do
      {outgoing(_pid, _name, node), state} ->
        state = update_in(state.outgoing[node], &(&1 && List.delete(&1, ref)))
        {:noreply, state}
      {incoming(node, remote_ref), state} ->
        state = update_in(state.incoming[node], &(&1 && List.delete(&1, ref)))
        Topology.send(topology, node, monitor, {:remote_down, remote_ref, reason})
        {:noreply, state}
      {nil, state} ->
        {:noreply, state}
    end
  end
end
