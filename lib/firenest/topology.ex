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
  via the `child_spec/1` function, alongside the proper adapter.
  All topologies are also locally named.

  Firenest ships with a default topology called `Firenest.Topology.Erlang`
  that uses the Erlang distribution to build a fully meshed topology.
  """

  @typedoc "An atom identifying the topology name."
  @type t :: atom

  @typedoc "How named processes are identified by topology."
  @type name :: atom

  @doc """
  Returns the child specification for a topology.

  When started, the topology must create an ETS table with the same
  name as the topology and register the key `:adapter` under it,
  pointing to a module that implements the topology callbacks.
  """
  @callback child_spec(keyword()) :: Supervisor.child_spec()

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
  Syncs the given `pid` across the topology using its name.
  """
  @callback sync_named(t, pid) :: {:ok, [{node, id :: term}]} | {:error, {:already_synced, pid}}

  @doc """
  Returns the child specification for a topology.

  The `:adapter` and `:name` keys are required as part of `options`.
  All other keys have their semantics dictated by the adapter.

  ## Examples

  This is used to start the topology as part of your supervision tree:

      {Firenest.Topology, topology: MyApp.Topology, adapter: Firenest.Topology.Erlang}

  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    name = options[:name]
    {adapter, options} = Keyword.pop(options, :adapter)

    unless adapter && name do
      raise ArgumentError, "Firenest.Topology.child_spec/1 expects :adapter and :name as options"
    end

    adapter.child_spec(options)
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
  name is not known.

  However, keep in mind `:ok` does not guarantee the message
  was delivered nor processed by the receiving `name`, since
  `name` may have disconnected by the time we send (although we
  don't know it yet).
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
  def connect(topology, node) when is_atom(topology) and is_atom(node) do
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
  def disconnect(topology, node) when is_atom(topology) and is_atom(node) do
    adapter!(topology).disconnect(topology, node)
  end

  @doc """
  Syncs the given `pid` across the topology using its name.

  This function is the building block for building static services
  on top of the topology. It allows the current process to know whenever
  another process with the same name goes up or down in the topology
  as long as processes call `sync_named/2`.

  This function returns `{:ok, nodes}` in case the given pid has not
  been synced yet, `{:error, {:already_synced, pid}}` otherwise.
  `nodes` is a list of tuples with the first element with the node
  name as an atom and the second element is a term used to version
  that node name. Only the nodes that are known to have a service
  with the same `name` running and that have already called `sync_named/2`
  will be included in the list.

  Once this function is called, the given process `pid` will receive
  two messages with the following guarantees:

    * `{:named_up, node, id, name}` is delivered whenever a process
      with name `name` is up on the given `node-id` pair. The message
      is guaranteed to be delivered after the node is added to the list
      returned by `nodes/2`.

  * `{:named_down, node, id, name}` is delivered whenever a process
      with name `name` is down on the given `node-id` pair. It can be
      deivered when such processes crashes or when there is a disconnection.
      The message is guaranteed to be delivered after the node is removed
      from the list returned by `nodes/2`. Note the topology may not
      necessarily guarantee that no messages are received from `name`
      after this message is sent.

  In a case node loses connection and reconnects (either due to network
  partitions or because it crashed), a `:named_down` for that node is
  guaranteed to be delivered before `:named_up` event.
  """
  @spec sync_named(t, pid) :: {:ok, [{node, id :: term}]} | {:error, {:already_synced, pid}}
  def sync_named(topology, pid) when is_pid(pid) do
    adapter!(topology).sync_named(topology, pid)
  end

  @doc """
  Gets the adapter for the topology.

  Expects the topology to be running, otherwise it raises.
  """
  def adapter!(name) do
    try do
      :ets.lookup_element(name, :adapter, 2)
    catch
      :error, :badarg -> raise "could not find topology named #{inspect name}"
    end
  end
end
