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

  ## Custom dispatching

  Firenest.PubSub allows developers to perform custom dispatching
  by passing a `dispatcher` module to the broadcast functions.
  The dispatcher must be available on all nodes running the PubSub
  system. The `dispatch/3` function of the given module will be
  invoked with the subscriptions entries, the broadcaster identifier
  and the message to broadcast and it is responsible for local message
  deliveries.

  You may want to use the dispatcher to perform special delivery for
  certain subscriptions. This can be done by passing a `value` during
  subscriptions. For instance, Phoenix Channels use a custom `value`
  to provide "fastlaning", allowing messages broadcast to thousands
  or even millions of users to be encoded once and written directly
  to sockets instead of being encoded per channel.
  """

  @typedoc "An atom identifying the pubsub system."
  @type t :: atom()
  @type topic :: term()
  @type from :: pid()
  @type dispatcher :: module

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

  """
  @spec child_spec(options) :: Supervisor.child_spec()
        when options: [
               name: t,
               topology: Firenest.Topology.t(),
               partitions: pos_integer()
             ]
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

  The `value` argument is used for those implementing custom
  dispatching as explained in the "Custom Disapatching" section
  in the module docs. Unless you are implementing custom
  dispatching rules, you can safely ignore the `value` argument.
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
  @spec broadcast(t, topic | [topic], term, dispatcher) :: :ok | {:error, term}
  def broadcast(pubsub, topic, message, dispatcher \\ __MODULE__)
      when is_atom(pubsub) and is_atom(dispatcher) do
    topics = List.wrap(topic)
    {:ok, {topology, remote}} = Registry.meta(pubsub, :pubsub)
    broadcast = {:broadcast, topics, message, dispatcher}

    with :ok <- Firenest.Topology.broadcast(topology, remote, broadcast) do
      dispatch(pubsub, :none, topics, message, dispatcher)
    end
  end

  @doc """
  Broadcasts the given `message` on `topic` in `pubsub`.

  Returns `:ok` or raises `Firenest.PubSub.BroadcastError` in case of
  failures in the distributed brodcast.
  """
  @spec broadcast!(t, topic | [topic], term, dispatcher) :: :ok | no_return
  def broadcast!(pubsub, topic, message, dispatcher \\ __MODULE__) do
    case broadcast(pubsub, topic, message, dispatcher) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast!/3 failed with #{inspect(error)}"
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
  @spec broadcast_from(t, pid, topic | [topic], term, dispatcher) :: :ok | {:error, term()}
  def broadcast_from(pubsub, pid, topic, message, dispatcher \\ __MODULE__)
      when is_atom(pubsub) and is_pid(pid) and is_atom(dispatcher) do
    topics = List.wrap(topic)
    {:ok, {topology, remote}} = Registry.meta(pubsub, :pubsub)
    broadcast = {:broadcast, topics, message, dispatcher}

    with :ok <- Firenest.Topology.broadcast(topology, remote, broadcast) do
      dispatch(pubsub, pid, topics, message, dispatcher)
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
  @spec broadcast_from!(t, pid, topic | [topic], term, dispatcher) :: :ok | no_return
  def broadcast_from!(pubsub, pid, topic, message, dispatcher \\ __MODULE__) do
    case broadcast_from(pubsub, pid, topic, message, dispatcher) do
      :ok -> :ok
      {:error, error} -> raise BroadcastError, "broadcast_from!/4 failed with #{inspect(error)}"
    end
  end

  @doc """
  Broadcasts locally the given `message` on `topic` in `pubsub`.

  Returns `:ok`.
  """
  @spec local_broadcast(t, topic | [topic], term, dispatcher) :: :ok
  def local_broadcast(pubsub, topic, message, dispatcher \\ __MODULE__)
      when is_atom(pubsub) and is_atom(dispatcher) do
    dispatch(pubsub, :none, List.wrap(topic), message, dispatcher)
  end

  @doc """
  Broadcasts locally the given `message` on `topic` in `pubsub` from the given `pid`.

  By passing a `pid`, `Firenest.PubSub` the message won't be broadcast
  to `pid`. This is typically invoked with `pid == self()` so messages
  are not delivered to the broadcasting process.
  """
  @spec local_broadcast_from(t, pid, topic | [topic], term, dispatcher) :: :ok
  def local_broadcast_from(pubsub, from, topic, message, dispatcher \\ __MODULE__)
      when is_atom(pubsub) and is_pid(from) and is_atom(dispatcher) do
    dispatch(pubsub, from, List.wrap(topic), message, dispatcher)
  end

  @doc false
  def dispatch(entries, from, message) do
    Enum.each(entries, fn
      {pid, _} when pid == from -> :ok
      {pid, _} -> send(pid, message)
    end)
  end

  defp dispatch(pubsub, from, topics, message, dispatcher) do
    mfa = {dispatcher, :dispatch, [from, message]}

    for topic <- topics do
      Registry.dispatch(pubsub, topic, mfa)
    end

    :ok
  end
end

defmodule Firenest.PubSub.Dispatcher do
  @moduledoc false

  use GenServer

  def start_link({name, pubsub}) do
    GenServer.start_link(__MODULE__, pubsub, name: name)
  end

  def init(pubsub) do
    {:ok, pubsub}
  end

  def handle_info({:broadcast, topics, message, dispatcher}, pubsub) do
    mfargs = {dispatcher, :dispatch, [:none, message]}

    for topic <- topics do
      Registry.dispatch(pubsub, topic, mfargs)
    end

    {:noreply, pubsub}
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
    partitions =
      options[:partitions] || System.schedulers_online() |> Kernel./(4) |> Float.ceil() |> trunc()

    remote = Module.concat(pubsub, "Dispatcher")

    registry = [
      meta: [pubsub: {topology, remote}],
      partitions: partitions,
      keys: :duplicate,
      name: pubsub
    ]

    children = [
      {Registry, registry},
      {Firenest.PubSub.Dispatcher, {remote, pubsub}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
