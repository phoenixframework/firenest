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

  Firenest ships with a default topology called `Firenest.Topology.PG2`.
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
  Starts a topology with name `topology` and the given `options`.

  The `:adapter` key is required as part of `options`. All other
  keys have their semantics dictated by the adapter.

  It returns `{:ok, pid}` where `pid` represents a supervisor or
  `{:error, term}`.

  ## Examples

  Most times the topology is started as part of your supervision tree:

      supervisor(Firenest.Topology, [MyApp.Topology, [adapter: Firenest.Topology.PG2]])

  which is equivalent to calling:

      Firenest.Topology.start_link(MyApp.Topology, adapter: Firenest.Topology.PG2)

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

  defp adapter!(name) do
    try do
      :ets.lookup_element(name, :adapter, 2)
    catch
      :error, :badarg -> raise "could not find topology named #{inspect name}"
    end
  end
end
