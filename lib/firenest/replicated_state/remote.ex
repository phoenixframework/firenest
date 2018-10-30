defmodule Firenest.ReplicatedState.Remote do
  defstruct pending: %{}, clocks: %{}, clock: 0, event: nil, tag: nil

  def new(:ignore) do
    %__MODULE__{event: &event_ignore/3, tag: :ignore}
  end

  # Reconnections are dead until we have permdown
  def up(%__MODULE__{clocks: clocks} = state, ref, clock) do
    case clocks do
      # Reconnection, try to catch up
      %{^ref => old_clock} when clock > old_clock ->
        {:catch_up, ref, clock, old_clock, state}

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
        {:catch_up, ref, clock, 0, state}
    end
  end

  def down()

  def catch_up()

  def broadcast(%__MODULE__{pending: pending, clock: clock, tag: tag} = state) do
    new_state = %{state | pending: %{}, clock: clock + 1}
    {{tag, clock, Map.to_list(pending)}, new_state}
  end

  def handle_broadcast(%__MODULE__{clocks: clocks, tag: tag}, ref, {tag, clock, data}) do
    handle_broadcast(clocks, ref, clock, data)
  end

  def handle_broadcast(%__MODULE__{tag: local_tag}, ref, {remote_tag, _, _}) do
    {:error, {:different_tag, ref, local_tag, remote_tag}}
  end

  def handle_catch_up()

  def local_put(state, key) do
    event(state, key, :put)
  end

  def local_delete(state, key) do
    event(state, key, :delete)
  end

  def local_update(state, key) do
    event(state, key, :update)
  end

  defp handle_broadcast(clocks, ref, clock, data) do

  end

  defp event(%__MODULE__{pending: pending, event: process} = state, key, event) do
    pending = process.(pending, key, event)
    %{state | pending: pending}
  end

  def event_ignore(pending, key, :put) do
    Map.put(pending, key, :put)
  end

  def event_ignore(pending, key, :delete) do
    case pending do
      %{^key => :put} -> Map.delete(pending, key)
      %{} -> Map.put(pending, key, :delete)
    end
  end

  def event_ignore(pending, key, :update) do
    case pending do
      %{^key => :put} -> pending
      %{^key => :update} -> pending
      %{} -> Map.put(pending, key, :update)
    end
  end
end
