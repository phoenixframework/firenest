defmodule Firenest.Topology do
  @moduledoc """
  Defines and interacts with Firenest topologies.

  The topology is the building block in Firenest. It specifies:

    * How nodes are connected and discovered
    * How failures are handled (temporary and permanent)
    * How messages are send across nodes
    * How messages are broadcast in the cluster

  The topology allows named processes running on other nodes
  to be reached via broadcasts or direct messages. The named
  processes currently are identified by the local atom name.

  An instance of `Firenest.Topology` must be started per node,
  via the `start_link/2` function, alongside the proper adapter.
  All topologies are also locally named.

  Firenest ships with a default topology called `Firenest.Topology.Erlang`
  that uses the Erlang distribution to build a fully meshed topology.

  ## Subscription

  It is possible for a process to subscribe to events whenever a given
  topology changes by calling `subscribe/2`. The following events are
  delivered with the following guarantees:

    * `{:nodeup, node}` is delivered to the subscribed process whenever
      a new node comes up. The message is guaranteed to be delivered
      after the node is added to the list returned by `nodes/2`. There
      is no guarantee the `{:nodeup, node}` will be delivered before
      any messages from that node.

  * `{:nodedown, node}` is delivered to the subscribed process whenever
      a known node is down. The message is guaranteed to be delivered
      after the node is removed from the list returned by `nodes/2`.
      The nodedown notification is guaranteed to be delivered after all
      messages from that node (although it is not guaranteed to be delivered
      before all monitoring signals).

  In a case node loses connection and reconnects (either due to network
  partitions or because it crashed), a `:nodedown` for that node is
  guaranteed to be delivered before `:nodeup` event.
  """

  @typedoc "An atom identifying the topology name."
  @type t :: atom

  @typedoc "How named processes are identified by topology."
  @type name :: atom

  @doc """
  Starts a `topology`.

  Implementation-wise, the topology must create an ETS table
  with the same as the topology and register the key `:adapter`
  under it, pointing to a module that implements the topology
  callbacks.
  """
  @callback start_link(t, keyword()) :: {:ok, pid} | {:error, term}

  @doc """
  Returns the name of the current node in `topology`.
  """
  @callback node(t) :: node()

  @doc """
  Returns all other nodes in the `topology` (does not include the current node).
  """
  @callback nodes(t) :: [node()]

  @doc """
  Broadcasts `message` to all processes named `name` on all other nodes in `topology`.
  """
  @callback broadcast(t, name, message :: term) :: :ok | {:error, term}

  @doc """
  Sends a `message` to the process named `name` in `node` running on the `topology`.
  """
  @callback send(t, node, name, message :: term) :: :ok | {:error, term}

  @doc """
  Asks the topology to connect to the given node.
  """
  @callback connect(t, node) :: true | false | :ignored

  @doc """
  Asks the topology to disconnect from the given node.
  """
  @callback disconnect(t, node) :: true | false | :ignored

  @doc """
  Subscribes `pid` to the `topology` `:nodeup` and `:nodedown` events.
  """
  @callback subscribe(t, pid) :: reference

  @doc """
  Unsubscribes `ref` from the `topology` events.
  """
  @callback unsubscribe(t, reference) :: :ok

  @doc """
  Starts a topology with name `topology` and the given `options`.

  The `:adapter` key is required as part of `options`. All other
  keys have their semantics dictated by the adapter.

  It returns `{:ok, pid}` where `pid` represents a supervisor or
  `{:error, term}`.

  ## Examples

  Most times the topology is started as part of your supervision tree:

      supervisor(Firenest.Topology, [MyApp.Topology, [adapter: Firenest.Topology.Erlang]])

  which is equivalent to calling:

      Firenest.Topology.start_link(MyApp.Topology, adapter: Firenest.Topology.Erlang)

  """
  @spec start_link(t, keyword()) :: {:ok, pid} | {:error, term}
  def start_link(topology, options) when is_atom(topology) do
    {adapter, options} = Keyword.pop(options, :adapter)

    unless adapter do
      raise ArgumentError, "Firenest.Topology.start_link/2 expects :adapter as option"
    end

    adapter.start_link(topology, options)
  end

  @doc """
  Returns the name of the current node in `topology`.

      iex> Firenest.Topology.node(MyApp.Topology)
      :foo@example

  If the node is not connected to any other node, it may return
  `:nonode@nohost`.
  """
  @spec node(t) :: node()
  def node(topology) when is_atom(topology) do
    adapter!(topology).node(topology)
  end

  @doc """
  Returns all other nodes in the `topology` (does not include the current node).

      iex> Firenest.Topology.nodes(MyApp.Topology)
      [:bar@example, :baz@example]

  """
  @spec nodes(t) :: [node()]
  def nodes(topology) when is_atom(topology) do
    adapter!(topology).nodes(topology)
  end

  @doc """
  Broadcasts `message` to all processes named `name` on all other nodes in `topology`.

  The message is not broadcast to the process named `name`
  in the current node.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec broadcast(t, name, message :: term) :: :ok | {:error, term}
  def broadcast(topology, name, message) when is_atom(topology) and is_atom(name) do
    adapter!(topology).broadcast(topology, name, message)
  end

   @doc """
  Sends `message` to processes named `name` in `node`.

  Returns `:ok` or `{:error, reason}`. In particular,
  `{:error, :noconnection}` must be returned if the node
  name is not known. However, keep in mind `:ok` does
  not guarantee the message was delivered nor processed
  by the `name`, since `name` may have disconnected by
  the time we send (although we don't know it yet).
  """
  @spec send(t, node, name, message :: term) :: :ok | {:error, term}
  def send(topology, node, name, message) when is_atom(topology) and is_atom(node) and is_atom(name) do
    adapter!(topology).send(topology, node, name, message)
  end

  @doc """
  Asks the topology to connect to the given node.

  It returns `true` in case of success (or if the node is already
  connected), `false` in case of failure and `:ignored` if the node
  is not online or if the operation is not supported.
  """
  @spec connect(t, node) :: true | false | :ignored
  def connect(topology, node) do
    adapter!(topology).connect(topology, node)
  end

  @doc """
  Asks the topology to disconnect from the given node.

  It returns `true` if the nodes are no longer connected. This
  means it will also return `true` if nodes were never connected in
  the first place. It returns `:ignored` if the node is not online
  or if the operation is not supported.
  """
  @spec disconnect(t, node) :: true | false | :ignored
  def disconnect(topology, node) do
    adapter!(topology).disconnect(topology, node)
  end

  @doc """
  Subscribes `pid` to the `topology` `:nodeup` and `:nodedown` events.

  See the module documentation for a description of events and their
  guarantees.
  """
  @spec subscribe(t, pid) :: reference
  def subscribe(topology, pid) do
    adapter!(topology).subscribe(topology, pid)
  end

  @doc """
  Unsubscribes `ref` from the `topology` events.

  See the module documentation for a description of events and their
  guarantees.
  """
  @spec unsubscribe(t, reference) :: :ok
  def unsubscribe(topology, ref) do
    adapter!(topology).unsubscribe(topology, ref)
  end

  defp adapter!(name) do
    try do
      :ets.lookup_element(name, :adapter, 2)
    catch
      :error, :badarg -> raise "could not find topology named #{inspect name}"
    end
  end
end
