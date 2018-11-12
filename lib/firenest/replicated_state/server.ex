defmodule Firenest.ReplicatedState.Server do
  @moduledoc false
  use Firenest.SyncedServer

  alias Firenest.SyncedServer
  alias Firenest.ReplicatedState.{Store, Remote, Handler}

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

    remote_changes = Keyword.get(opts, :remote_changes, :ignore)
    broadcast_timeout = Keyword.get(opts, :broadcast_timeout, 50)
    broadcast_fun = fn -> Process.send_after(self(), :broadcast_timeout, broadcast_timeout) end
    remote = Remote.new(remote_changes, broadcast_fun)

    delayed_fun = &Process.send_after(self(), {:update, &1, &2, &3}, &4)
    # The `mode` should be read from init instead of options
    handler = Handler.new(handler, opts, delayed_fun)

    {:ok,
     %{
       store: store,
       handler: handler,
       remote: remote,
     }}
  end

  @impl true
  def handshake_data(%{remote: remote}), do: Remote.clock(remote)

  @impl true
  def handle_call({:put, key, pid, arg}, _from, state) do
    %{store: store, handler: handler, remote: remote} = state

    link(pid)

    unless Store.present?(store, key, pid) do
      case Handler.local_put(handler, arg, key, pid) do
        {:put, value, delta, handler} ->
          remote = Remote.local_put(remote, key, pid, value)
          store = Store.local_put(store, key, pid, value, delta)
          {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}

        {:delete, value, _delta, handler} ->
          remote = Remote.local_put(remote, key, pid, value)
          remote = Remote.local_delete(remote, key, pid)
          handler = Handler.local_delete(handler, [value])
          {:reply, :ok, %{state | handler: handler, remote: remote}}
      end
    else
      {:reply, {:error, :already_present}, state}
    end
  end

  def handle_call({:update, key, pid, arg}, _from, state) do
    %{store: store, handler: handler, remote: remote} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case Handler.local_update(handler, arg, key, pid, delta, value) do
          {:put, value, delta, handler} ->
            remote = Remote.local_update(remote, key, pid, value, delta)
            store = Store.local_update(store, key, pid, value, delta)
            {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}

          {:delete, value, delta, handler} ->
            remote = Remote.local_update(remote, key, pid, value, delta)
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                remote = Remote.local_delete(remote, key, pid)
                handler = Handler.local_delete(handler, [value])
                {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                remote = Remote.local_delete(remote, key, pid)
                handler = Handler.local_delete(handler, [value])
                {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}
            end
        end

      :error ->
        {:reply, {:error, :not_present}, state}
    end
  end

  def handle_call({:delete, key, pid}, _from, state) do
    %{store: store, handler: handler, remote: remote} = state

    case Store.local_delete(store, key, pid) do
      {:ok, value, store} ->
        remote = Remote.local_delete(remote, key, pid)
        handler = Handler.local_delete(handler, [value])
        {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}

      {:last_member, value, store} ->
        unlink_flush(pid)
        remote = Remote.local_delete(remote, key, pid)
        handler = Handler.local_delete(handler, [value])
        {:reply, :ok, %{state | store: store, handler: handler, remote: remote}}

      {:error, store} ->
        {:reply, {:error, :not_present}, %{state | store: store}}
    end
  end

  def handle_call({:delete, pid}, _from, state) do
    %{store: store, handler: handler} = state

    case Store.local_delete(store, pid) do
      {:ok, deletes, store} ->
        unlink_flush(pid)
        handler = Handler.local_delete(handler, deletes)
        # TODO: how do we handle remote in here?
        {:reply, :ok, %{state | store: store, handler: handler}}

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
    %{store: store, handler: handler} = state

    case Store.local_delete(store, pid) do
      {:ok, leaves, store} ->
        handler = Handler.local_delete(handler, leaves)
        # TODO: how do we handle remote in here?
        {:noreply, %{state | store: store, handler: handler}}

      {:error, store} ->
        {:stop, reason, %{state | store: store}}
    end
  end

  def handle_info(:broadcast_timeout, state) do
    %{remote: remote, handler: handler} = state

    prepare_delta = Handler.prepare_remote_delta_fun(handler)
    {data, remote} = Remote.broadcast(remote, prepare_delta)
    SyncedServer.remote_broadcast({:broadcast, data})
    {:noreply, %{state | remote: remote}}
  end

  def handle_info({:update, key, pid, arg}, state) do
    %{store: store, handler: handler} = state

    case Store.fetch(store, key, pid) do
      {:ok, value, delta} ->
        case Handler.local_update(handler, arg, key, pid, delta, value) do
          {:put, value, delta, handler} ->
            store = Store.local_update(store, key, pid, value, delta)
            {:noreply, %{state | store: store, handler: handler}}

          {:delete, value, _delta, handler} ->
            # TODO: this delta has to propagate remotely before delete
            case Store.local_delete(store, key, pid) do
              # The value returned from update is fresher
              {:ok, _value, store} ->
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                handler = Handler.local_delete(handler, [value])
                {:noreply, %{state | store: store, handler: handler}}

              {:last_member, _value, store} ->
                unlink_flush(pid)
                handler = Handler.local_delete(handler, [value])
                # state = schedule_broadcast_events(state, [{:leave, key, pid}])
                {:noreply, %{state | store: store, handler: handler}}
            end
        end

      :error ->
        # Must have been already deleted, ignore
        {:noreply, state}
    end
  end

  @impl true
  def handle_remote({:catch_up_req, data}, from, state) do
    %{remote: remote, store: store} = state

    get_all_local = fn -> Store.list_local(store) end
    reply = Remote.catch_up(remote, data, get_all_local)
    SyncedServer.remote_send(from, {:catch_up_rep, reply})
    {:noreply, state}
  end

  def handle_remote({:catch_up_rep, data}, from, state) do
    %{remote: remote, store: store, handler: handler} = state

    case Remote.handle_catch_up(remote, from, data) do
      {:insert, data, remote} ->
        store = Store.remote_update(store, from, data)
        {:noreply, %{state | remote: remote, store: store}}

      {:diff, puts, updates, deletes, remote} ->
        update_handler = &Handler.handle_remote_delta(handler, &1, &2)
        store = Store.remote_diff(store, puts, updates, deletes, update_handler)
        {:noreply, %{state | remote: remote, store: store}}

      {:ok, remote} ->
        {:noreply, %{state | remote: remote}}
    end
  end

  def handle_remote({:broadcast, data}, from, state) do
    %{remote: remote, store: store, hander: handler} = state

    case Remote.handle_broadcast(remote, from, data) do
      {:diff, puts, updates, deletes, remote} ->
        update_handler = &Handler.handle_remote_delta(handler, &1, &2)
        store = Store.remote_diff(store, puts, updates, deletes, update_handler)
        {:noreply, %{state | remote: remote, store: store}}

      {:catch_up, data, remote} ->
        SyncedServer.remote_send(from, {:catch_up_req, data})
        {:noreply, %{state | remote: remote}}

      {:ok, remote} ->
        {:noreply, %{state | remote: remote}}
    end
  end

  @impl true
  def handle_replica(change, remote_ref, state) do
    %{remote: remote, store: store} = state

    case remote_replica(change, remote_ref, remote) do
      {:ok, remote} ->
        {:noreply, %{state | remote: remote}}

      {:delete, remote} ->
        store = Store.remote_delete(store, remote_ref)
        {:noreply, %{state | remote: remote, store: store}}

      {:catch_up, data, remote} ->
        SyncedServer.remote_send(remote_ref, {:catch_up_req, data})
        {:noreply, %{state | remote: remote}}
    end
  end

  defp remote_replica({:up, clock}, ref, remote) do
    Remote.up(remote, ref, clock)
  end

  defp remote_replica(:down, ref, remote) do
    Remote.down(remote, ref)
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

  # defp schedule_broadcast_events(%{broadcast_timer: nil} = state, new_events) do
  #   %{broadcast_timeout: timeout, pending_events: events} = state
  #   timer = :erlang.start_timer(timeout, self(), :broadcast)
  #   %{state | broadcast_timer: timer, pending_events: new_events ++ events}
  # end

  # defp schedule_broadcast_events(%{} = state, new_events) do
  #   %{pending_events: events} = state
  #   %{state | pending_events: new_events ++ events}
  # end

  # defp request_catch_up(state, remote_ref, clock) do
  #   SyncedServer.remote_send(remote_ref, {:catch_up_req, clock})
  #   state
  # end

  # defp handle_events(%{values: values} = state, from, events) do
  #   {joins, leaves} =
  #     Enum.reduce(events, {[], []}, fn
  #       {:leave, key, pid}, {joins, leaves} ->
  #         leave = {{{key, pid}, from, :_}, [], [true]}
  #         {joins, [leave | leaves]}

  #       {:replace, key, pid, value}, {joins, leaves} ->
  #         join = {{key, pid}, from, value}
  #         {[join | joins], leaves}

  #       {:join, key, pid, value}, {joins, leaves} ->
  #         join = {{key, pid}, from, value}
  #         {[join | joins], leaves}
  #     end)

  #   :ets.insert(values, joins)
  #   :ets.select_delete(values, leaves)
  #   state
  # end

  # # TODO: detect leaves
  # # Is there a better way than to clean up and re-insert?
  # # This can be problematic for dirty reads!
  # defp handle_state_transfer(%{values: values} = state, from, clock, transfer) do
  #   %{remote_clocks: remote_clocks} = state
  #   delete_ms = [{{:_, from, :_}, [], [true]}]
  #   inserts = for {ets_key, value} <- transfer, do: {ets_key, from, value}
  #   :ets.select_delete(values, delete_ms)
  #   :ets.insert(values, inserts)
  #   %{state | remote_clocks: %{remote_clocks | from => clock}}
  # end

  # # TODO: handle catch-up with events
  # defp catch_up_reply(%{values: values}, clock) do
  #   local_ms = [{{:"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}]
  #   {:state_transfer, {clock, :ets.select(values, local_ms)}}
  # end
end
