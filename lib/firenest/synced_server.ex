defmodule Firenest.SyncedServer do
  @moduledoc """
  Server with state synchronised across topology.
  """
  use GenServer

  import Kernel, except: [send: 2]

  alias Firenest.Topology

  @type state() :: term()

  @type server() :: pid() | atom()

  @doc """
  Invoked when the server is started. `start_link/3` will block until it returns.

  See `c:GenServer.init/1` for explanation of return values.
  """
  @callback init(arg :: term()) ::
              {:ok, state}
              | {:ok, state(), timeout() | :hibernate}
              | {:stop, reason :: term()}

  @doc """
  Invoked to handle synchronous `call/3` messages. `call/3` will block until
  a reply is received (unless the call times out or nodes are disconnected).

  If not implemented, the server will crash on unexpected messages.

  See `c:GenServer.handle_call/3` for explanation of return values.
  """
  @callback handle_call(request :: term(), GenServer.from(), state()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout() | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term(), new_state: state(), reason: term()

  @doc """
  Invoked to handle simple messages.

  If not implemented, the server will log unexpected messages.

  See `c:GenServer.handle_info/2` for explanation of return values.
  """
  @callback handle_info(request :: term(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @doc """
  Invoked to handle messages sent with `remote_send/2` and `remote_broadcast/1`.

  If not implemented, the server will crash on unexpected messages.

  See `c:handle_info/2` for explanation of return values.
  """
  @callback handle_remote(request :: term(), from :: Topology.node_ref(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @doc """
  Invoked when a status of a remote synced server changes.

  When the `:up` signal is delivered, a two-step handshake
  was exchanged between replicas and the remote replica is
  ready to receive messages.

  If not implemented, the server will ignore unexpected messages.

  See `c:handle_info/2` for explanation of return values.
  """
  @callback handle_replica(:up | :down, Topology.node_ref(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @doc """
  Invoked when the server is about to exit. It should do any cleanup required.

  See `c:GenServer.terminate/2` for detailed explanation.
  """
  @callback terminate(reason, state()) :: term()
            when reason: :normal | :shutdown | {:shutdown, term()}

  @doc """
  Invoked in some cases to retrieve a formatted version of the
  `SyncedServer` status.

  See `c:GenServer.format_status/2` for detailed explanation.
  """
  @callback format_status(reason, pdict_and_state :: list()) :: term()
            when reason: :normal | :terminate

  @doc """
  Invoked to change the state of the `SyncedServer` when a different
  version of a module is loaded (hot code swapping) and the state’s
  term structure should be changed.

  See `c:GenServer.code_change/3` for detailed explanation.
  """
  @callback code_change(old_vsn, state :: term(), extra :: term()) ::
              {:ok, new_state :: term()} | {:error, reason :: term()} | {:down, term()}
            when old_vsn: term()

  @optional_callbacks [
    handle_call: 3,
    handle_info: 2,
    handle_remote: 3,
    handle_replica: 3,
    terminate: 2,
    format_status: 2,
    code_change: 3
  ]

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour Firenest.SyncedServer

      @doc """
      Returns a specification to start this module under a supervisor.
      See `Supervisor`.
      """
      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a `SyncedServer` process linked to the current process.

  This is often used to start the`SyncedServer` as part of a supervision tree.

  ## Options

    * `:name` - the name of the server to be started
    * `:topology` - the name of a `Firenest.Topology` that powers
      the distribution mechanism
    * `:timeout` - if present, the server is allowed to spend the given
      number of milliseconds initializing or it will be terminated and
      the start function will return `{:error, :timeout}`
    * `:debug` - if present, the corresponding function in the
      `:sys` module is invoked
    * `:spawn_opt` - if present, its value is passed as options to the
      underlying process as in `Process.spawn/4`

  """
  def start_link(mod, arg, opts) do
    name = Keyword.fetch!(opts, :name)
    topology = Keyword.fetch!(opts, :topology)
    GenServer.start_link(__MODULE__, {mod, arg, topology, name}, opts)
  end

  @doc """
  Makes a synchronous call to the server and waits for its reply.

  The client sends the given request to the server and waits until a reply
  arrives or a timeout occurs. `handle_call/3` will be called on the server
  to handle the request.

  See `GenServer.call/3` for additional information.
  """
  @spec call(server(), msg :: term(), timeout()) :: term()
  def call(server, msg, timeout \\ 5_000) do
    GenServer.call(server, msg, timeout)
  end

  @doc """
  Replies to a client.

  See `GenServer.reply/2` for additional information.
  """
  @spec reply(GenServer.from(), term()) :: :ok
  def reply(client, reply) do
    GenServer.reply(client, reply)
  end

  @doc """
  Sends `message` to the remote server in node identified by `to_node_ref`.

  This function can be used only inside callbacks of `SyncedServer`.

  The remote server will either receive the message or this server will
  receive a `:down` notification in `c:handle_replica/3`.
  """
  @spec remote_send(Topology.node_ref(), term()) :: :ok | {:error, term()}
  def remote_send(to_node_ref, message) do
    {topology, name, node_ref} = Process.get(__MODULE__)
    Topology.send(topology, to_node_ref, name, {__MODULE__, :message, message, node_ref})
  end

  @doc """
  Broadcasts `message` to all remote servers in all other nodes.

  This function can be used only inside callbacks of `SyncedServer`.
  """
  @spec remote_broadcast(term()) :: :ok | {:error, term()}
  def remote_broadcast(message) do
    {topology, name, node_ref} = Process.get(__MODULE__)
    Topology.broadcast(topology, name, {__MODULE__, :message, message, node_ref})
  end

  ## Implementation

  @impl true
  def init({mod, arg, topology, name}) do
    state = %{
      name: name,
      topology: topology,
      mod: mod,
      int: nil,
      awaiting_hello: [],
      awaiting_up: [],
      replicas: []
    }

    with {:ok, state} <- init_mod(mod, arg, state),
         {:ok, state, node_ref} <- sync_named(topology, name, state) do
      Process.put(__MODULE__, {topology, name, node_ref})
      {:ok, state}
    end
  end

  @impl true
  def handle_cast(msg, state) do
    {:stop, {:badcast, msg}, state}
  end

  @impl true
  def handle_call(msg, from, state) do
    case apply_callback(state, :handle_call, [msg, from]) do
      {:reply, reply, int} -> {:reply, reply, %{state | int: int}}
      {:reply, reply, int, timeout} -> {:reply, reply, %{state | int: int}, timeout}
      {:stop, reason, reply, int} -> {:stop, reason, reply, %{state | int: int}}
      other -> handle_common(other, state)
    end
  end

  @impl true
  def handle_info({:named_up, node_ref, name}, %{name: name} = state) do
    %{awaiting_up: awaiting, replicas: replicas} = state

    case delete_element(awaiting, node_ref) do
      {:ok, awaiting} ->
        result = apply_callback(state, :handle_replica, [:up, node_ref])
        handle_common(result, %{state | awaiting_up: awaiting, replicas: [node_ref | replicas]})

      :error ->
        {:noreply, %{state | awaiting_hello: [node_ref | awaiting]}}
    end
  end

  def handle_info({:named_down, node_ref, name}, %{name: name} = state) do
    %{awaiting_hello: awaiting, replicas: replicas} = state

    case delete_element(replicas, node_ref) do
      {:ok, replicas} ->
        result = apply_callback(state, :handle_replica, [:down, node_ref])
        handle_common(result, %{state | replicas: replicas})

      :error ->
        {:noreply, %{state | awaiting_hello: List.delete(awaiting, node_ref)}}
    end
  end

  def handle_info({__MODULE__, :hello, node_ref}, state) do
    %{awaiting_hello: awaiting, replicas: replicas} = state

    case delete_element(awaiting, node_ref) do
      {:ok, awaiting} ->
        result = apply_callback(state, :handle_replica, [:up, node_ref])

        handle_common(result, %{state | awaiting_hello: awaiting, replicas: [node_ref | replicas]})

      :error ->
        {:noreply, %{state | awaiting_up: [node_ref | awaiting]}}
    end
  end

  def handle_info({__MODULE__, :message, msg, node_ref}, %{replicas: replicas} = state) do
    if node_ref in replicas do
      result = apply_callback(state, :handle_remote, [msg, node_ref])
      handle_common(result, state)
    else
      {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    result = apply_callback(state, :handle_info, [msg])
    handle_common(result, state)
  end

  @impl true
  def terminate(reason, state) do
    apply_callback(state, :terminate, [reason])
    :ok
  end

  @impl true
  def code_change(old_vsn, %{mod: mod, int: int} = state, extra) do
    if function_exported?(mod, :code_change, 3) do
      try do
        mod.code_change(old_vsn, int, extra)
      catch
        :throw, value ->
          exit({{:nocatch, value}, System.stacktrace()})
      else
        {:ok, int} ->
          {:ok, %{state | int: int}}
      end
    else
      {:ok, state}
    end
  end

  @impl true
  def format_status(:normal, [pdict, %{mod: mod, int: int}]) do
    try do
      apply(mod, :format_status, [:normal, [pdict, int]])
    catch
      _, _ ->
        [{:data, [{'State', int}]}]
    else
      mod_status ->
        mod_status
    end
  end

  def format_status(:terminate, [pdict, %{mod: mod, int: int}]) do
    try do
      apply(mod, :format_status, [:terminate, [pdict, int]])
    catch
      _, _ ->
        int
    else
      mod_state ->
        mod_state
    end
  end

  defp handle_common(result, state) do
    case result do
      {:noreply, int} -> {:noreply, %{state | int: int}}
      {:noreply, int, timeout} -> {:noreply, %{state | int: int}, timeout}
      {:stop, reason, int} -> {:stop, reason, %{state | int: int}}
      other -> {:stop, {:bad_return_value, other}, state}
    end
  end

  defp init_mod(mod, arg, state) do
    Process.put(:"$initial_call", {mod, :init, 2})

    try do
      mod.init(arg)
    catch
      :throw, value ->
        reason = {{:nocatch, value}, System.stacktrace()}
        {:stop, reason}
    else
      {:ok, int} -> {:ok, %{state | int: int}}
      {:ok, int, timeout} -> {:ok, %{state | int: int}, timeout}
      {:stop, reason} -> {:stop, reason}
      :ignore -> :ignore
      other -> {:stop, {:bad_return_value, other}}
    end
  end

  defp sync_named(topology, name, state) do
    node_ref = Topology.node(topology)

    case Topology.sync_named(topology, self()) do
      {:ok, replicas} ->
        init_replicas(topology, name, replicas, node_ref)
        {:ok, %{state | awaiting_hello: replicas}, node_ref}

      {:error, error} ->
        {:stop, {:sync_named, error}}
    end
  end

  defp init_replicas(topology, name, replicas, node_ref) do
    Enum.each(replicas, &Topology.send(topology, &1, name, {__MODULE__, :hello, node_ref}))
  end

  defp apply_callback(%{mod: mod, int: int}, fun, args) do
    try do
      apply(mod, fun, args ++ [int])
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())

      :error, :undef when fun == :handle_info ->
        if function_exported?(mod, :handle_info, 2) do
          :erlang.raise(:error, :undef, System.stacktrace())
        else
          proc =
            case Process.info(self(), :registered_name) do
              {_, []} -> self()
              {_, name} -> name
            end

          pattern = 'Undefined handle_info/2 in ~p, process ~p received unexpected message: ~p~n'
          :error_logger.warning_msg(pattern, [mod, proc, hd(args)])
          {:noreply, int}
        end

      :error, :undef when fun == :handle_replica ->
        if function_exported?(mod, :handle_replica, 3) do
          :erlang.raise(:error, :undef, System.stacktrace())
        else
          {:noreply, int}
        end

      :error, :undef when fun == :terminate ->
        if function_exported?(mod, :terminate, 2) do
          :erlang.raise(:error, :undef, System.stacktrace())
        else
          :ok
        end
    end
  end

  defp delete_element(list, elem), do: delete_element(list, elem, [])

  defp delete_element([elem | rest], elem, acc),
    do: {:ok, Enum.reverse(acc, rest)}

  defp delete_element([], _elem, _acc),
    do: :error

  defp delete_element([other | rest], elem, acc),
    do: delete_element(rest, elem, [other | acc])
end
