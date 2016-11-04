defmodule Firenest.Topology do
  def start_link(name, opts) do
    {adapter, opts} = Keyword.pop(opts, :adapter)

    unless adapter do
      raise ArgumentError, "Firenest.Topology.start_link/2 expects an :adapter as option"
    end

    adapter.start_link(name, opts)
  end
end
