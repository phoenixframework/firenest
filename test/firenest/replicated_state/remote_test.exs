defmodule Firenest.ReplicatedState.RemoteTest do
  use ExUnit.Case, async: true

  alias Firenest.ReplicatedState.Remote

  @moduletag remote_changes: :ignore

  setup %{remote_changes: changes} do
    parent = self()
    broadcast = fn -> send(parent, :broadcast) end
    remote = Remote.new(changes, broadcast, 1)
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

      {data, _remote} = Remote.broadcast(remote, &identity/1)
      assert {:ignore, 0, {puts, _updates = [], _deletes = []}} = data
      assert [{{:b, self()}, 2}, {{:a, self()}, 1}] == puts
    end

    test "local_update/5", %{remote: remote} do
      remote =
        remote
        |> Remote.local_update(:a, self(), 1, 2)
        |> Remote.local_put(:b, self(), 3)
        |> Remote.local_update(:b, self(), 4, 5)

      {data, _remote} = Remote.broadcast(remote, &identity/1)
      assert {:ignore, 0, {puts, updates, _deletes = []}} = data
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
      assert {:ignore, 0, {_puts = [], _updates = [], deletes}} = data
      assert [{:c, self()}, {:a, self()}] == deletes
    end
  end

  defp identity(x), do: x
end
