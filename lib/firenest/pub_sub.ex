defmodule Firenest.PubSub do
  @moduledoc """
  A distributed pubsub implementation.
  """

  @type pubsub :: atom()
  @type topic :: binary() | atom()
  @type from :: pid()

  defmodule BroadcastError do
    defexception [:message]
  end

  defmodule Dispatcher do
    @moduledoc false

    use GenServer

    def dispatch(entries, from, message) do
      Enum.each entries, fn
        {pid, _} when pid == from ->
          :ok
        {pid, _} ->
          send(pid, message)
      end
    end

    def start_link(name, pubsub, module, function) do
      GenServer.start_link(__MODULE__, {pubsub, module, function}, name: name)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_info({:broadcast, topics, message}, state) do
      {pubsub, module, function} = state
      for topic <- topics do
        Registry.dispatch(pubsub, topic, {module, function, [:none, message]})
      end
      {:noreply, state}
    end
  end

  @spec start_link(pubsub, keyword()) :: {:ok, pid} | {:error, term}
  def start_link(pubsub, options) when is_atom(pubsub)  do
    topology = options[:topology]
    partitions = options[:partitions] || (System.schedulers_online |> div(4) |> max(1))
    {module, function} = options[:dispatcher] || {Dispatcher, :dispatch}

    unless topology do
      raise ArgumentError, "Firenest.Topology.start_link/2 expects :topology as option"
    end

    supervisor = Module.concat(pubsub, "Supervisor")
    dispatcher = Module.concat(pubsub, "Dispatcher")
    registry = [meta: [pubsub: {topology, dispatcher, module, function}],
                partitions: partitions]

    import Supervisor.Spec

    children = [
      supervisor(Registry, [:duplicate, pubsub, registry]),
      worker(Dispatcher, [dispatcher, pubsub, module, function])
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: supervisor)
  end

  @spec subscribe(pubsub, topic, term) :: :ok
  def subscribe(pubsub, topic, value \\ nil) when is_atom(pubsub) do
    {:ok, _} = Registry.register(pubsub, topic, value)
    :ok
  end

  @spec unsubscribe(pubsub, topic) :: :ok
  def unsubscribe(pubsub, topic) when is_atom(pubsub) do
    Registry.unregister(pubsub, topic)
  end

  @spec broadcast(pubsub, topic | [topic], term) :: :ok | {:error, term}
  def broadcast(pubsub, topic, message) when is_atom(pubsub) do
    topics = List.wrap(topic)
    {:ok, {topology, dispatcher, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, :none, topics, message, module, function)
    Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topics, message})
  end

  @spec broadcast!(pubsub, topic | [topic], term) :: :ok | no_return
  def broadcast!(pubsub, topic, message) do
    case broadcast(pubsub, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast!/3 failed with #{inspect error}"
    end
  end

  @spec broadcast_from(pubsub, pid, topic | [topic], term) :: :ok
  def broadcast_from(pubsub, from, topic, message) when is_atom(pubsub) and is_pid(from) do
    topics = List.wrap(topic)
    {:ok, {topology, dispatcher, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, from, topics, message, module, function)
    Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topics, message})
  end

  @spec broadcast_from!(pubsub, pid, topic | [topic], term) :: :ok | no_return
  def broadcast_from!(pubsub, from, topic, message) do
    case broadcast_from(pubsub, from, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast_from!/4 failed with #{inspect error}"
    end
  end

  @spec local_broadcast(pubsub, topic | [topic], term) :: :ok
  def local_broadcast(pubsub, topic, message) when is_atom(pubsub) do
    topics = List.wrap(topic)
    {:ok, {_, _, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, :none, topics, message, module, function)
  end

  @spec local_broadcast_from(pubsub, pid, topic | [topic], term) :: :ok
  def local_broadcast_from(pubsub, from, topic, message) when is_atom(pubsub) and is_pid(from) do
    topics = List.wrap(topic)
    {:ok, {_, _, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, from, topics, message, module, function)
  end

  defp dispatch(pubsub, from, topics, message, module, function) do
    mfa = {module, function, [from, message]}
    for topic <- topics, do: Registry.dispatch(pubsub, topic, mfa)
    :ok
  end
end
