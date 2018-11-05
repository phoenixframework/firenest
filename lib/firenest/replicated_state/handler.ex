defmodule Firenest.ReplicatedState.Handler do
  defstruct mod: nil, init_delta: nil, config: nil, delayed_fun: nil

  def new(mod, mod_opts, delayed_fun) do
    {init_delta, config} = mod.init(mod_opts)
    %__MODULE__{mod: mod, init_delta: init_delta, config: config, delayed_fun: delayed_fun}
  end

  def local_put(%__MODULE__{} = state, arg, key, pid) do
    %{mod: mod, config: config, init_delta: delta, delayed_fun: delayed} = state

    case mod.local_put(arg, delta, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta, state}

      {delta, value, {:update_after, update, time}} ->
        delayed.(key, pid, update, time)
        {:put, value, delta, state}
    end
  end

  def local_update(%__MODULE__{} = state, arg, key, pid, local_delta, value) do
    %{mod: mod, config: config, delayed_fun: delayed} = state

    case mod.local_update(arg, local_delta, value, config) do
      {delta, value} ->
        {:put, value, delta, state}

      {delta, value, :delete} ->
        {:delete, value, delta, state}

      {delta, value, {:update_after, update, time}} ->
        delayed.(key, pid, update, time)
        {:put, value, delta, state}
    end
  end

  def local_delete(%__MODULE__{} = state, deletes) do
    %{mod: mod, config: config} = state

    Enum.each(deletes, &mod.local_delete(&1, config))
    state
  end
end
