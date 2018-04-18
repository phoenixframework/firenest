defmodule Firenest.PubSub do
  @moduledoc """
  A distributed pubsub implementation.

  The PubSub implementation runs on top of a `Firenest.Topology`
  and uses Elixir's `Registry` to provide a scalable dispatch
  implementation.

  ## Example

  PubSub is typically set up as part of your supervision tree
  alongside the desired topology:

      children = [
        {Firenest.Topology, name: MyApp.Topology, adapter: Firenest.Topology.Erlang},
        {Firenest.PubSub, name: MyApp.PubSub, topology: MyApp.Topology}
      ]

  Once the topology and pubsub processes are started, processes
  may subscribe, unsubscribe and broadcast messages:

      # Subscribe the current process to a given topic
      Firenest.PubSub.subscribe(MyApp.PubSub, "lobby:messages")

      # Broadcasts a message
      Firenest.PubSub.broadcast(MyApp.PubSub, "lobby:messages", "hello world")

  PubSub will always broadcast to all nodes in the topology,
  even if they are not running the PubSub service. In case you
  want to broadcast to a subset of your topology, consider creating
  multiple topologies.
  """

  @typedoc "An atom identifying the pubsub system."
  @type t :: atom()
  @type topic :: term()
  @type from :: pid()

  defmodule BroadcastError do
    defexception [:message]
  end

  @doc """
  Returns a child specifiction for pubsub with the given `options`.

  The `:name` and `:topology` keys are required as part of `options`.
  `:name` refers to the name of the pubsub to be started and `:topology`
  must point to a topology started by `Firenest.Topology`.
  The remaining options are described below.

  ## Options

    * `:name` - the name of the pubsub to be started
    * `:topology` - the name of a `Firenest.Topology` that powers
      the distribution mechanism
    * `:partitions` - the number of partitions under the pubsub system.
      Partitioning provides vertical scalability on machines with multiple
      cores, allowing subscriptions and broadcasts to happen concurrently.
      By default uses one partition for every 4 cores.
    * `:dispatcher` - a module and function tuple that is called every time
      there is a broadcast and must perform local message deliveries. It
      receives the subscriptions entries, the broadcaster identifier and the
      message to broadcast

  """
  @spec child_spec(options) :: Supervisor.child_spec when
        options: [name: t,
                  topology: Firenest.Topology.t,
                  partitions: pos_integer(),
                  dispatcher: {module, function}]
  defdelegate child_spec(options), to: Firenest.PubSub.Supervisor

  @doc """
  Returns all topics the `pid` is subscribed to in `pubsub`.
  """
  @spec topics(t, pid) :: [topic]
  def topics(pubsub, pid) do
    Registry.keys(pubsub, pid)
  end

  @doc """
  Subscribes the current process to `topic` in `pubsub`.

  A process may subscribe to the same topic more than once.
  In such cases, messages will be delivered twice.

  ## The dispatching value

  An optional `value` may be given which may be used by custom
  dispatchers (see `child_spec/1`). For instance, Phoenix Channels
  use a custom `value` to provide "fastlaning", allowing messages
  broadcast to thousands or even millions of users to be encoded
  once and written directly to sockets instead of being encoded per
  channel.

  Unless you are implementing custom dispatching rules, you can
  safely ignore the `value` argument.
  """
  @spec subscribe(t, topic, term) :: :ok
  def subscribe(pubsub, topic, value \\ nil) when is_atom(pubsub) do
    {:ok, _} = Registry.register(pubsub, topic, value)
    :ok
  end

  @doc """
  Unsubscribe the current process from `topic` in `pubsub`.

  In case the current process is subscribed to the topic multiple times,
  this call will unsubscribe all entries at once.
  """
  @spec unsubscribe(t, topic) :: :ok
  def unsubscribe(pubsub, topic) when is_atom(pubsub) do
    Registry.unregister(pubsub, topic)
  end

  @doc """
  Broadcasts the given `message` on `topic` in `pubsub`.

  Returns `:ok` or `{:error, reason}` in case of failures in
  the distributed brodcast.
  """
  @spec broadcast(t, topic | [topic], term) :: :ok | {:error, term}
  def broadcast(pubsub, topic, message) when is_atom(pubsub) do
    topics = List.wrap(topic)
    {:ok, {topology, dispatcher, module, function}} = Registry.meta(pubsub, :pubsub)
    with :ok <- Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topics, message}) do
      dispatch(pubsub, :none, topics, message, module, function)
    end
  end

  @doc """
  Broadcasts the given `message` on `topic` in `pubsub`.

  Returns `:ok` or raises `Firenest.PubSub.BroadcastError` in case of
  failures in the distributed brodcast.
  """
  @spec broadcast!(t, topic | [topic], term) :: :ok | no_return
  def broadcast!(pubsub, topic, message) do
    case broadcast(pubsub, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast!/3 failed with #{inspect error}"
    end
  end

  @doc """
  Broadcasts the given `message` on `topic` in `pubsub` from the given `pid`.

  By passing a `pid`, `Firenest.PubSub` the message won't be broadcast
  to `pid`. This is typically invoked with `pid == self()` so messages
  are not delivered to the broadcasting process.

  Returns `:ok` or `{:error, reason}` in case of failures in
  the distributed brodcast.
  """
  @spec broadcast_from(t, pid, topic | [topic], term) :: :ok
  def broadcast_from(pubsub, pid, topic, message) when is_atom(pubsub) and is_pid(pid) do
    topics = List.wrap(topic)
    {:ok, {topology, dispatcher, module, function}} = Registry.meta(pubsub, :pubsub)
    with :ok <- Firenest.Topology.broadcast(topology, dispatcher, {:broadcast, topics, message}) do
      dispatch(pubsub, pid, topics, message, module, function)
    end
  end

  @doc """
  Broadcasts the given `message` on `topic` in `pubsub` from the given `pid`.

  By passing a `pid`, `Firenest.PubSub` the message won't be broadcast
  to `pid`. This is typically invoked with `pid == self()` so messages
  are not delivered to the broadcasting process.

  Returns `:ok` or raises `Firenest.PubSub.BroadcastError` in case of
  failures in the distributed brodcast.
  """
  @spec broadcast_from!(t, pid, topic | [topic], term) :: :ok | no_return
  def broadcast_from!(pubsub, pid, topic, message) do
    case broadcast_from(pubsub, pid, topic, message) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast_from!/4 failed with #{inspect error}"
    end
  end

  @doc """
  Broadcasts locally the given `message` on `topic` in `pubsub`.

  Returns `:ok`.
  """
  @spec local_broadcast(t, topic | [topic], term) :: :ok
  def local_broadcast(pubsub, topic, message) when is_atom(pubsub) do
    topics = List.wrap(topic)
    {:ok, {_, _, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, :none, topics, message, module, function)
  end

  @doc """
  Broadcasts locally the given `message` on `topic` in `pubsub` from the given `pid`.

  By passing a `pid`, `Firenest.PubSub` the message won't be broadcast
  to `pid`. This is typically invoked with `pid == self()` so messages
  are not delivered to the broadcasting process.
  """
  @spec local_broadcast_from(t, pid, topic | [topic], term) :: :ok
  def local_broadcast_from(pubsub, from, topic, message) when is_atom(pubsub) and is_pid(from) do
    topics = List.wrap(topic)
    {:ok, {_, _, module, function}} = Registry.meta(pubsub, :pubsub)
    dispatch(pubsub, from, topics, message, module, function)
  end

  defp dispatch(pubsub, from, topics, message, module, function) do
    mfa = {module, function, [from, message]}
    Enum.each(topics, &Registry.dispatch(pubsub, &1, mfa))
    :ok
  end
end

defmodule Firenest.PubSub.Dispatcher do
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

  def start_link({name, pubsub, module, function}) do
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

defmodule Firenest.PubSub.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(options) do
    pubsub = options[:name]
    topology = options[:topology]

    unless pubsub && topology do
      raise ArgumentError,
        "Firenest.PubSub.child_spec/1 expects :name and :topology as options"
    end

    supervisor = Module.concat(pubsub, "Supervisor")
    Supervisor.start_link(__MODULE__, {pubsub, topology, options}, name: supervisor)
  end

  def init({pubsub, topology, options}) do
    partitions = options[:partitions] ||
                 (System.schedulers_online |> Kernel./(4) |> Float.ceil() |> trunc())
    {module, function} = options[:dispatcher] || {Firenest.PubSub.Dispatcher, :dispatch}

    dispatcher = Module.concat(pubsub, "Dispatcher")
    registry = [meta: [pubsub: {topology, dispatcher, module, function}],
                partitions: partitions,
                keys: :duplicate,
                name: pubsub]

    children = [
      {Registry, registry},
      {Firenest.PubSub.Dispatcher, {dispatcher, pubsub, module, function}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
