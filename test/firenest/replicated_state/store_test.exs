defmodule Firenest.ReplicatedState.StoreTest do
  use ExUnit.Case, async: true

  alias Firenest.ReplicatedState.Store

  setup %{test: test} do
    %Store{} = store = Store.new(test)
    other = spawn_link(fn -> Process.sleep(:infinity) end)
    [store: store, other: other]
  end

  test "empty store", %{store: store} do
    assert Store.list(store, :a) == []
    assert Store.list_local(store) == []
    refute Store.present?(store, :a, self())
  end

  test "list with entries", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)

    assert Store.list(store, :a) == [1]
    assert Store.list(store, :b) == [2]
    assert Store.list(store, :c) == []
    assert Store.list_local(store) == [{{:a, self()}, 1}, {{:b, other}, 2}]
  end

  test "present?/3", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)

    assert Store.present?(store, :a, self())
    assert Store.present?(store, :b, other)
    refute Store.present?(store, :c, self())
    refute Store.present?(store, :a, other)
  end

  test "fetch/3", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)

    assert Store.fetch(store, :a, self()) == {:ok, 1, 1}
    assert Store.fetch(store, :b, other) == {:ok, 2, 2}
    assert Store.fetch(store, :c, self()) == :error
    assert Store.fetch(store, :a, other) == :error
  end

  test "local_update/5", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)
      |> Store.local_update(:a, self(), 3, 3)
      |> Store.local_update(:b, other, 4, 4)

    assert Store.fetch(store, :a, self()) == {:ok, 3, 3}
    assert Store.fetch(store, :b, other) == {:ok, 4, 4}
  end

  test "local_delete/2", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)

    another = spawn_link(fn -> Process.sleep(:infinity) end)

    assert Store.local_delete(store, another) == :error

    assert {:ok, [1], store} = Store.local_delete(store, self())
    refute Store.present?(store, :a, self())
    assert Store.present?(store, :b, other)
  end

  test "local_delete/3", %{store: store, other: other} do
    store =
      store
      |> Store.local_put(:a, self(), 1, 1)
      |> Store.local_put(:b, other, 2, 2)
      |> Store.local_put(:c, self(), 3, 3)

    another = spawn_link(fn -> Process.sleep(:infinity) end)

    assert Store.local_delete(store, :a, another) == :error

    assert {:ok, 1, store} = Store.local_delete(store, :a, self())
    assert {:last_member, 3, store} = Store.local_delete(store, :c, self())
    refute Store.present?(store, :a, self())
    refute Store.present?(store, :c, self())
    assert Store.present?(store, :b, other)
  end

  test "remote_update/3", %{store: store} do
    node_ref = {:node1, 1}
    data = [{{:a, remote_pid(:node1, 1)}, 1}]
    store = Store.remote_update(store, node_ref, data)

    assert Store.list(store, :a) == [1]

    store = Store.local_put(store, :a, self(), 2, 2)

    assert Store.list(store, :a) == [1, 2]

    new_data = [{{:b, remote_pid(:node1, 1)}, 3}]
    store = Store.remote_update(store, node_ref, new_data)

    assert Store.list(store, :a) == [2]
  end

  test "remote_delete/2", %{store: store} do
    data1 = [{{:a, remote_pid(:node1, 1)}, 1}]
    data2 = [{{:a, remote_pid(:node2, 1)}, 2}]

    store =
      store
      |> Store.remote_update({:node1, 1}, data1)
      |> Store.remote_update({:node2, 1}, data2)
      |> Store.remote_delete({:node1, 1})

    assert Store.list(store, :a) == [2]
  end

  test "remote_diff/5", %{store: store} do
    parent = self()

    update_handler = fn delta, value ->
      send(parent, {:update, delta, value})
      delta + value
    end

    data = [{{:a, remote_pid(:node1, 1)}, 1}, {{:a, remote_pid(:node1, 2)}, 2}]
    store = Store.remote_update(store, {:node1, 1}, data)

    puts = [{{:a, remote_pid(:node1, 3)}, 3}]
    updates = [{{:a, remote_pid(:node1, 2)}, 4}]
    deletes = [{:a, remote_pid(:node1, 1)}]
    store = Store.remote_diff(store, puts, updates, deletes, update_handler)

    assert Store.list(store, :a) == [6, 3]
    assert_received {:update, 4, 2}
  end

  defp remote_pid(node, num) do
    <<131, 100, node::binary>> = :erlang.term_to_binary(node)
    :erlang.binary_to_term(<<131, 103, 100, node::binary, num::8*4, 0::8*4, 0::8*1>>)
  end
end
