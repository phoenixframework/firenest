defmodule Firenest.ReplicatedState.RemoteTest do
  use ExUnit.Case, async: true

  alias Firenest.ReplicatedState.Remote

  @moduletag remote_changes: :ignore

  defmacrop assert_received_times(times, pattern) do
    receives = List.duplicate(quote(do: assert_received(unquote(pattern))), times)
    receives ++ [quote(do: refute_received(unquote(pattern)))]
  end

  setup %{remote_changes: changes} do
    parent = self()
    %Remote{} = remote = Remote.new(changes, fn -> send(parent, :broadcast) end, 1)
    [remote: remote]
  end

  test "up and down", %{remote: remote} do
    assert {:ok, remote} = Remote.up(remote, :node1, 0)
    assert {:catch_up, 0, remote} = Remote.up(remote, :node2, 5)
    assert {:delete, :node1, _remote} = Remote.down(remote, :node1)
  end

  describe "remote_changes: :ignore" do
    @describetag remote_changes: :ignore

    test "local_put/4", %{remote: remote} do
      remote =
        remote
        |> Remote.local_put(:a, self(), 1)
        |> Remote.local_put(:b, self(), 2)

      assert_received_times(1, :broadcast)

      {data, _remote} = Remote.broadcast(remote, &identity/1)
      assert {:ignore, 1, {puts, _updates = [], _deletes = []}} = data
      assert [{{:b, self()}, 2}, {{:a, self()}, 1}] == puts
    end

    test "local_update/5", %{remote: remote} do
      remote =
        remote
        |> Remote.local_update(:a, self(), 1, 2)
        |> Remote.local_put(:b, self(), 3)
        |> Remote.local_update(:b, self(), 4, 5)

      {data, _remote} = Remote.broadcast(remote, &identity/1)
      assert {:ignore, 1, {puts, updates, _deletes = []}} = data
      assert [{{:b, self()}, 4}] == puts
      assert [{{:a, self()}, 2}] == updates
    end

    test "local_delete/3", %{remote: remote} do
      remote =
        remote
        |> Remote.local_update(:a, self(), 1, 2)
        |> Remote.local_put(:b, self(), 3)
        |> Remote.local_delete(:a, self())
        |> Remote.local_delete(:b, self())
        |> Remote.local_delete(:c, self())

      {data, _remote} = Remote.broadcast(remote, &identity/1)
      assert {:ignore, 1, {_puts = [], _updates = [], deletes}} = data
      assert [{:c, self()}, {:a, self()}] == deletes
    end

    test "(handle_)catch_up/3 with deltas", %{remote: remote} do
      other = Remote.new(:ignore, fn -> nil end, 1)

      {_, remote} =
        remote
        |> Remote.local_put(:a, self(), 1)
        |> Remote.broadcast(&identity/1)

      assert {:catch_up, request, other} = connect(remote, :remote, other)
      reply = Remote.catch_up(remote, request, fn -> flunk("should not get here") end)

      assert {:diff, puts, updates, deletes, other} =
               Remote.handle_catch_up(other, :remote, reply)

      assert [{{:a, self()}, 1}] == puts
      assert [] == updates
      assert [] == deletes

      assert Remote.clock_for(other, :remote) == Remote.clock(remote)
    end

    test "(handle_)catch_up/3 with state transfer", %{} do
      remote = Remote.new(:ignore, fn -> nil end, 0)
      other = Remote.new(:ignore, fn -> nil end, 1)

      {_, remote} =
        remote
        |> Remote.local_put(:a, self(), 1)
        |> Remote.broadcast(&identity/1)

      assert {:catch_up, request, other} = connect(remote, :remote, other)
      data = [{{:a, self()}, 1}]
      reply = Remote.catch_up(remote, request, fn -> data end)

      assert {:replace, ^data, other} = Remote.handle_catch_up(other, :remote, reply)
      assert Remote.clock_for(other, :remote) == Remote.clock(remote)
    end

    test "handle_broadcast/3", %{remote: remote} do
      other = Remote.new(:ignore, fn -> nil end, 1)

      assert {:ok, other} = connect(remote, :remote, other)

      {broadcast, remote} =
        remote
        |> Remote.local_put(:a, self(), 1)
        |> Remote.broadcast(&identity/1)

      assert {:diff, puts, updates, deletes, other} =
               Remote.handle_broadcast(other, :remote, broadcast)

      assert [{{:a, self()}, 1}] == puts
      assert [] == updates
      assert [] == deletes

      assert Remote.clock_for(other, :remote) == Remote.clock(remote)
    end
  end

  defp identity(x), do: x

  defp connect(remote, id, other) do
    clock = Remote.clock(remote)
    Remote.up(other, id, clock)
  end
end
