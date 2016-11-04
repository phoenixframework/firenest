defmodule Firenest.PubSub do
  @moduledoc """
  A distributed pubsub implementation.
  """

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

  def subscribe(pubsub, topic, value \\ nil) do
    {:ok, _} = Registry.register(pubsub, topic, value)
    :ok
  end

  def unsubscribe(pubsub, topic) do
    Registry.unregister(pubsub, topic)
  end

  def broadcast(pubsub, topic, message) do
    {topology, dispatcher, module, function} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, :none, topic, message, module, function)
    Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topic, message})
  end

  def broadcast!(pubsub, topic, message) do
    case broadcast(pubsub, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast!/3 failed with #{inspect error}"
    end
  end

  def broadcast_from(pubsub, from, topic, message) do
    {topology, dispatcher, module, function} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, from, topic, message, module, function)
    Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topic, message})
  end

  def broadcast_from!(pubsub, from, topic, message) do
    case broadcast_from(pubsub, from, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast_from!/4 failed with #{inspect error}"
    end
  end

  def local_broadcast(pubsub, topic, message) do
    {_, _, module, function} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, :none, topic, message, module, function)
  end

  def local_broadcast_from(pubsub, pid, topic, message) do
    {_, _, module, function} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, pid, topic, message, module, function)
  end

  defp dispatch(pubsub, from, topic, message, module, function) do
    Registry.dispatch(pubsub, topic, {module, function, [from, message]})
  end
end

