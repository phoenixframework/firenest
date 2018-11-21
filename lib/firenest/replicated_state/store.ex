defmodule Firenest.ReplicatedState.Store do
  @moduledoc false

  defstruct [:values, :pids]

  # Local data is stored in the values table in the following format:
  #
  #     {{key, pid}, value, delta}
  #
  # Remote data is stored as:
  #
  #     {{key, pid}, value}
  #
  # To recognise the node that remote data is coming from we can use
  # the `node/1` function in the match spec. This saves on space of
  # explicitly storing node and whole-node operations should be rare
  # in practice. We can ignore the version part of node_ref, since
  # we're guaranteed to get a nodedown from the old version of the node
  # before it comes back up with a new version - we can ignore the
  # version when storing the data.
  #
  # The pids table stores data as {key, pid} with key position of 2.
  # This allows having the same data format in both tables and save
  # on some data shuffling.

  def new(name) do
    values = :ets.new(name, [:named_table, :protected, :ordered_set, read_concurrency: true])
    pids = :ets.new(__MODULE__.Pids, [:private, :duplicate_bag, keypos: 2])

    %__MODULE__{values: ets_whereis(values), pids: pids}
  end

  def list(%__MODULE__{values: values}, key) do
    local = {{{key, :_}, :"$1", :_}, [], [:"$1"]}
    remote = {{{key, :_}, :"$1"}, [], [:"$1"]}
    :ets.select(values, [local, remote])
  end

  def list_local(%__MODULE__{values: values}) do
    ms = [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}]
    :ets.select(values, ms)
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
        :error

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
        :error

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

  def remote_delete(%__MODULE__{values: values} = state, {node, _}) when is_atom(node) do
    remote_delete_values(values, node)
    state
  end

  def remote_update(%__MODULE__{values: values} = state, {node, _}, data) when is_atom(node) do
    remote_delete_values(values, node)
    :ets.insert(values, data)
    state
  end

  def remote_diff(%__MODULE__{values: values} = state, puts, updates, deletes, update_handler) do
    delete_ms = for key <- deletes, do: {{key, :_}, [], [true]}
    puts = Enum.reduce(updates, puts, &[prepare_update(values, &1, update_handler) | &2])
    :ets.insert(values, puts)
    :ets.select_delete(values, delete_ms)
    state
  end

  defp remote_delete_values(values, node) do
    ms = [{{{:_, :"$1"}, :_}, [{:"=:=", {:node, :"$1"}, {:const, node}}], [true]}]
    :ets.select_delete(values, ms)
  end

  defp prepare_update(values, {key, delta}, update_handler) do
    value = :ets.lookup_element(values, key, 2)
    new_value = update_handler.(delta, value)
    {key, new_value}
  end

  if function_exported?(:ets, :whereis, 1) do
    defp ets_whereis(table), do: :ets.whereis(table)
  else
    defp ets_whereis(table), do: table
  end
end
