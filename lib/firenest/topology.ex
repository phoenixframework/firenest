defmodule Firenest.Topology do
  @typedoc "An atom identifying the topology name."
  @type topology :: atom

  @callback start_link(topology, keyword()) :: {:ok, pid} | {:error, term}
  @callback node(topology) :: node()
  @callback nodes(topology) :: [node()]
  @callback broadcast(topology, name :: atom, message :: term) :: :ok

  def start_link(name, opts) do
    {adapter, opts} = Keyword.pop(opts, :adapter)

    unless adapter do
      raise ArgumentError, "Firenest.Topology.start_link/2 expects an :adapter as option"
    end

    adapter.start_link(name, opts)
  end

  def node(topology) when is_atom(topology) do
    adapter!(topology).node(topology)
  end

  def nodes(topology) when is_atom(topology) do
    adapter!(topology).nodes(topology)
  end

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
