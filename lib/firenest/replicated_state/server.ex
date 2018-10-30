defmodule Firenest.ReplicatedState.Server do
  @moduledoc false
  use Firenest.SyncedServer

  alias Firenest.SyncedServer
  alias Firenest.ReplicatedState.{Store, Remote}

  def child_spec({name, topology, handler, opts}) do
    server_opts = [name: name, topology: topology]

    %{
      id: name,
      start: {SyncedServer, :start_link, [__MODULE__, {name, handler, opts}, server_opts]}
    }
  end

  @impl true
  def init({name, handler, opts}) do
    Process.flag(:trap_exit, true)
    store = Store.new(name)
    broadcast_timeout = Keyword.get(opts, :broadcast_timeout, 50)
    remote_changes = Keyword.get(opts, :remote_changes, :ignore)
    remote = Remote.new(remote_changes)
    {initial_delta, config} = handler.init(opts)

    {:ok,
     %{
       store: store,
       config: config,
       initial_delta: initial_delta,
       handler: handler,
       broadcast_timer: nil,
       broadcast_timeout: broadcast_timeout,
       remote: remote,
       clock: 0,
       pending_events: []
     }}
  end

  @impl true
  def handshake_data(%{clock: clock}), do: clock

  @impl true
  def handle_call({:put, key, pid, arg}, _from, state) do
    %{store: store} = state

    link(pid)

    unless Store.present?(store, key, pid) do
      case local_put(arg, key, pid, state) do
        {:put, value, delta, state} ->
          store = Store.local_put(store, key, pid, value, delta)
          {:reply, :ok, %{state | store: store}}

        {:delete, value, _delta} ->
          # TODO: this delta has to propagate remotely before delete
          state = local_delete([value], state)
          {:reply, :ok, state}
      end
    else
      {:reply, {:error, :already_present}, state}
    end
  end

  def handle_call({:update, key, pid, arg}, _from, state) do
    %{store: store} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case local_update(arg, key, pid, delta, value, state) do
          {:put, value, delta, state} ->
            store = Store.local_update(store, key, pid, value, delta)
            {:reply, :ok, %{state | store: store}}

          {:delete, value, _delta} ->
            # TODO: this delta has to propagate remotely before delete
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                state = local_delete([value], state)
                {:reply, :ok, %{state | store: store}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                state = local_delete([value], state)
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                {:reply, :ok, %{state | store: store}}
            end
        end

      :error ->
        {:reply, {:error, :not_present}, state}
    end
  end

  def handle_call({:delete, key, pid}, _from, state) do
    %{store: store} = state

    case Store.local_delete(store, key, pid) do
      {:ok, value, store} ->
        # state = schedule_broadcast_events(state, [{:leave, key, pid}])
        state = local_delete([value], state)
        {:reply, :ok, %{state | store: store}}

      {:last_member, value, store} ->
        unlink_flush(pid)
        state = local_delete([value], state)
        # state = schedule_broadcast_events(state, [{:leave, key, pid}])
        {:reply, :ok, %{state | store: store}}

      {:error, store} ->
        {:reply, {:error, :not_present}, %{state | store: store}}
    end
  end

  def handle_call({:delete, pid}, _from, state) do
    %{store: store} = state

    case Store.local_delete(store, pid) do
      {:ok, leaves, store} ->
        unlink_flush(pid)
        state = local_delete(leaves, state)
        # state = schedule_broadcast_events(state, leaves)
        {:reply, :ok, %{state | store: store}}

      {:error, store} ->
        {:reply, {:error, :not_member}, %{state | store: store}}
    end
  end

  def handle_call({:list, key}, _from, state) do
    %{store: store} = state

    read = fn -> Store.list(store, key) end

    {:reply, read, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    %{store: store} = state

    case Store.local_delete(store, pid) do
      {:ok, leaves, store} ->
        state = local_delete(leaves, state)
        # state = schedule_broadcast_events(state, leaves)
        {:noreply, %{state | store: store}}

      {:error, store} ->
        {:stop, reason, %{state | store: store}}
    end
  end

  def handle_info({:timeout, timer, :broadcast}, %{broadcast_timer: timer} = state) do
    %{pending_events: events, clock: clock} = state
    clock = clock + 1
    SyncedServer.remote_broadcast({:events, clock, events})
    {:noreply, %{state | clock: clock, pending_events: [], broadcast_timer: nil}}
  end

  def handle_info({:update, key, pid, arg}, state) do
    %{store: store} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case local_update(arg, key, pid, delta, value, state) do
          {:put, value, delta, state} ->
            store = Store.local_update(store, key, pid, value, delta)
            {:noreply, %{state | store: store}}

          {:delete, value, _delta} ->
            # TODO: this delta has to propagate remotely before delete
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                state = local_delete([value], state)
                {:noreply, %{state | store: store}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                state = local_delete([value], state)
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                {:noreply, %{state | store: store}}
            end
        end

      :error ->
        # Must have been already deleted, ignore
        {:noreply, state}
    end
  end

  # @impl true
  # def handle_remote({:catch_up_req, clock}, from, state) do
  #   {mode, data} = catch_up_reply(state, clock)
  #   SyncedServer.remote_send(from, {:catch_up, mode, data})
  #   {:noreply, state}
  # end

  # def handle_remote({:catch_up, :state_transfer, {clock, transfer}}, from, state) do
  #   state = handle_state_transfer(state, from, clock, transfer)
  #   {:noreply, state}
  # end

  # def handle_remote({:events, remote_clock, events}, from, state) do
  #   %{remote_clocks: remote_clocks} = state
  #   local_clock = Map.fetch!(remote_clocks, from)

  #   if remote_clock == local_clock + 1 do
  #     remote_clocks = %{remote_clocks | from => remote_clock}
  #     state = handle_events(state, from, events)
  #     {:noreply, %{state | remote_clocks: remote_clocks}}
  #   else
  #     {:noreply, request_catch_up(state, from, local_clock)}
  #   end
  # end

  # @impl true
  # def handle_replica({:up, remote_clock}, remote_ref, state) do
  #   %{remote_clocks: remote_clocks} = state

  #   case remote_clocks do
  #     %{^remote_ref => old_clock} when remote_clock > old_clock ->
  #       # Reconnection, try to catch up
  #       {:noreply, request_catch_up(state, remote_ref, old_clock)}

  #     %{^remote_ref => old_clock} ->
  #       # Reconnection, no remote state change, skip catch up
  #       # Assert for sanity
  #       true = old_clock == remote_clock
  #       {:noreply, state}

  #     %{} when remote_clock == 0 ->
  #       # New node, no state, don't catch up
  #       state = %{state | remote_clocks: Map.put(remote_clocks, remote_ref, 0)}
  #       {:noreply, state}

  #     %{} ->
  #       # New node, catch up
  #       state = %{state | remote_clocks: Map.put(remote_clocks, remote_ref, 0)}
  #       {:noreply, request_catch_up(state, remote_ref, 0)}
  #   end
  # end

  # def handle_replica(:down, remote_ref, state) do
  #   %{values: values, remote_clocks: remote_clocks} = state
  #   delete_ms = [{{:_, remote_ref, :_}, [], [true]}]
  #   :ets.select_delete(values, delete_ms)
  #   {:noreply, %{state | remote_clocks: Map.delete(remote_clocks, remote_ref)}}
  # end

  defp local_put(arg, key, pid, state) do
    %{handler: handler, config: config, initial_delta: delta} = state

    case handler.local_put(arg, delta, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta}

      {delta, value, {:update_after, update, time}} ->
        Process.send_after(self(), {:update, key, pid, update}, time)
        {:put, value, delta, state}
    end
  end

  defp local_update(arg, key, pid, local_delta, value, state) do
    %{handler: handler, config: config} = state

    case handler.local_update(arg, local_delta, value, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta}

      {delta, value, {:update_after, update, time}} ->
        Process.send_after(self(), {:update, key, pid, update}, time)
        {:put, value, delta, state}
    end
  end

  defp local_delete(leaves, state) do
    %{handler: handler, config: config} = state

    Enum.each(leaves, &handler.local_delete(&1, config))
    state
  end

  defp link(:partition), do: true
  defp link(pid), do: Process.link(pid)

  defp unlink_flush(:partition), do: true

  defp unlink_flush(pid) do
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, _} -> true
    after
      0 -> true
    end
  end

  defp schedule_broadcast_events(%{broadcast_timer: nil} = state, new_events) do
    %{broadcast_timeout: timeout, pending_events: events} = state
    timer = :erlang.start_timer(timeout, self(), :broadcast)
    %{state | broadcast_timer: timer, pending_events: new_events ++ events}
  end

  defp schedule_broadcast_events(%{} = state, new_events) do
    %{pending_events: events} = state
    %{state | pending_events: new_events ++ events}
  end

  defp request_catch_up(state, remote_ref, clock) do
    SyncedServer.remote_send(remote_ref, {:catch_up_req, clock})
    state
  end

  defp handle_events(%{values: values} = state, from, events) do
    {joins, leaves} =
      Enum.reduce(events, {[], []}, fn
        {:leave, key, pid}, {joins, leaves} ->
          leave = {{{key, pid}, from, :_}, [], [true]}
          {joins, [leave | leaves]}

        {:replace, key, pid, value}, {joins, leaves} ->
          join = {{key, pid}, from, value}
          {[join | joins], leaves}

        {:join, key, pid, value}, {joins, leaves} ->
          join = {{key, pid}, from, value}
          {[join | joins], leaves}
      end)

    :ets.insert(values, joins)
    :ets.select_delete(values, leaves)
    state
  end

  # TODO: detect leaves
  # Is there a better way than to clean up and re-insert?
  # This can be problematic for dirty reads!
  defp handle_state_transfer(%{values: values} = state, from, clock, transfer) do
    %{remote_clocks: remote_clocks} = state
    delete_ms = [{{:_, from, :_}, [], [true]}]
    inserts = for {ets_key, value} <- transfer, do: {ets_key, from, value}
    :ets.select_delete(values, delete_ms)
    :ets.insert(values, inserts)
    %{state | remote_clocks: %{remote_clocks | from => clock}}
  end

  # TODO: handle catch-up with events
  defp catch_up_reply(%{values: values}, clock) do
    local_ms = [{{:"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}]
    {:state_transfer, {clock, :ets.select(values, local_ms)}}
  end
end
