defmodule Firenest.ReplicatedState.Store do
  defstruct [:values, :pids]

  def new(name) do
    values = :ets.new(name, [:named_table, :protected, :ordered_set])
    pids = :ets.new(__MODULE__.Pids, [:duplicate_bag, keypos: 2])

    %__MODULE__{values: ets_whereis(values), pids: pids}
  end

  def list(%__MODULE__{values: values}, key) do
    local = {{{key, :_}, :"$1", :_}, [], [:"$1"]}
    # remote = {{{key, :_}, :_, :"$1"}, [], [:"$1"]}
    :ets.select(values, [local])
  end

  def present?(%__MODULE__{values: values}, key, pid) do
    ets_key = {key, pid}
    :ets.member(values, ets_key)
  end

  def fetch(%__MODULE__{values: values}, key, pid) do
    ets_key = {key, pid}

    case :ets.match(values, {ets_key, :"$1", :"$2"}) do
      [[value, local_delta]] -> {:ok, value, local_delta}
      [] -> :error
    end
  end

  def local_put(%__MODULE__{values: values, pids: pids} = state, key, pid, value, local_delta) do
    ets_key = {key, pid}

    true = :ets.insert_new(values, {ets_key, value, local_delta})
    :ets.insert(pids, ets_key)
    state
  end

  def local_delete(%__MODULE__{values: values, pids: pids} = state, key, pid) do
    ets_key = {key, pid}
    ms = [{ets_key, [], [true]}]

    case :ets.select_delete(pids, ms) do
      0 ->
        {:error, state}

      1 ->
        [{_, value, _}] = :ets.take(values, ets_key)

        if :ets.member(pids, pid) do
          {:ok, value, state}
        else
          {:last_member, value, state}
        end
    end
  end

  def local_delete(%__MODULE__{values: values, pids: pids} = state, pid) do
    case :ets.take(pids, pid) do
      [] ->
        {:error, state}

      list ->
        delete_ms = for ets_key <- list, do: {{ets_key, :_, :_}, [], [true]}
        select_ms = for ets_key <- list, do: {{ets_key, :"$1", :_}, [], [:"$1"]}
        data = :ets.select(values, select_ms)
        :ets.select_delete(values, delete_ms)
        {:ok, data, state}
    end
  end

  def local_update(%__MODULE__{values: values} = state, key, pid, value, local_delta) do
    ets_key = {key, pid}

    :ets.insert(values, {ets_key, value, local_delta})
    state
  end

  if function_exported?(:ets, :whereis, 1) do
    defp ets_whereis(table), do: :ets.whereis(table)
  else
    defp ets_whereis(table), do: table
  end
end
