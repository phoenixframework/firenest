defmodule Firenest.Test.EvalState do
  @behaviour Firenest.ReplicatedState

  @impl true
  def init(opts) do
    {0, opts}
  end

  @impl true
  def local_put(fun, delta, config) when is_function(fun), do: fun.(delta, config)
  def local_put(data, delta, _config), do: {delta, data}

  @impl true
  def local_delete(fun, config) when is_function(fun), do: fun.(config)
  def local_delete(_data, _config), do: :ok

  @impl true
  def local_update(fun, delta, state, config) when is_function(fun),
    do: fun.(delta, state, config)
end
