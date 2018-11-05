defmodule Firenest.ReplicatedState.Remote do
  defstruct pending: %{}, clocks: %{}, clock: 0, tag: nil, deltas: %{}

  def new(:ignore) do
    %__MODULE__{tag: :ignore}
  end

  def clock(%__MODULE__{clock: clock}), do: clock

  # Reconnections are dead until we have permdown
  def up(%__MODULE__{clocks: clocks} = state, ref, clock) do
    case clocks do
      # Reconnection, try to catch up
      %{^ref => old_clock} when clock > old_clock ->
        {:catch_up, {clock, old_clock}, state}

      # Reconnection, no remote changes
      %{^ref => old_clock} ->
        # Assert for sanity
        true = old_clock == clock
        {:ok, state}

      # New node, no state
      %{} when clock == 0 ->
        {:ok, %{state | clocks: Map.put(clocks, ref, clock)}}

      # New node, catch up
      %{} ->
        {:catch_up, {clock, 0}, state}
    end
  end

  # Right now down means permdown
  def down(state, ref) do
    permdown(state, ref)
  end

  def permdown(%__MODULE__{clocks: clocks} = state, ref) do
    clocks = Map.delete(clocks, ref)
    {:purge, ref, %{state | clocks: clocks}}
  end

  def catch_up(%__MODULE__{clock: current} = state, {clock, old_clock}, state_getter)
      when old_clock < clock and clock <= current do
    %{deltas: deltas, tag: tag} = state

    if Map.has_key?(deltas, old_clock) do
      {:deltas, tag, Enum.flat_map(old_clock..clock, &Map.fetch!(deltas, &1))}
    else
      {:transfer_state, {:state_transfer, tag, current, state_getter.()}}
    end
  end

  def broadcast(%__MODULE__{pending: pending, clock: clock, tag: tag} = state, state_getter) do
    deltas = prepare_deltas(tag, pending, state_getter)
    new_state = %{state | pending: %{}, clock: clock + 1}
    {{tag, clock, deltas}, new_state}
  end

  def handle_catch_up(%__MODULE__{tag: tag} = state, from, {:deltas, tag, deltas}) do
  end

  def handle_catch_up(%__MODULE__{tag: tag} = state, from, {:state_transfer, tag, clock, state}) do
  end

  def handle_broadcast(%__MODULE__{clocks: clocks, tag: tag}, ref, {tag, clock, data}) do
    # handle_broadcast(tag, clocks, ref, clock, data)
  end

  def handle_broadcast(%__MODULE__{tag: local_tag}, ref, {remote_tag, _, _}) do
    {:error, {:different_tag, ref, local_tag, remote_tag}}
  end

  def local_put(state, key, pid) do
    event(state, key, pid, :put)
  end

  def local_delete(state, key, pid) do
    event(state, key, pid, :delete)
  end

  def local_update(state, key, pid) do
    event(state, key, pid, :update)
  end

  defp event(%__MODULE__{pending: pending, tag: tag} = state, key, pid, event) do
    pending =
      case tag do
        :ignore -> event_ignore(pending, key, pid, event)
      end

    %{state | pending: pending}
  end

  def event_ignore(pending, key, pid, :put) do
    Map.put(pending, {key, pid}, :put)
  end

  def event_ignore(pending, key, pid, :delete) do
    pending_key = {key, pid}

    case pending do
      %{^pending_key => :put} -> Map.delete(pending, pending_key)
      %{} -> Map.put(pending, pending_key, :delete)
    end
  end

  def event_ignore(pending, key, pid, :update) do
    pending_key = {key, pid}

    case pending do
      %{^pending_key => :put} -> pending
      %{} -> Map.put(pending, pending_key, :update)
    end
  end

  defp prepare_deltas(:ignore, pending, getter) do
    Enum.map(pending, &prepare_ignore_delta(&1, getter))
  end

  defp prepare_ignore_delta({{key, pid}, :put}, getter) do
    {:put, key, getter.(key, pid)}
  end

  defp prepare_ignore_delta({{key, pid}, :update}, getter) do
    {:put, key, getter.(key, pid)}
  end

  defp prepare_ignore_delta({{key, _pid}, :delete}, _getter) do
    {:delete, key}
  end
end
