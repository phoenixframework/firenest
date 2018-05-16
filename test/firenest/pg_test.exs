defmodule Firenest.PGTest do
  use ExUnit.Case, async: true

  alias Firenest.PG

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  setup %{test: test, topology: topology} do
    start_supervised!({PG, name: test, topology: topology})
    {:ok, pg: test}
  end

  describe "join/5" do
    test "adds process", %{pg: pg} do
      assert PG.join(pg, :foo, :bar, self(), :baz) == :ok
      assert [{:bar, :baz}] == PG.members(pg, :foo)
    end

    test "rejects double joins", %{pg: pg} do
      assert PG.join(pg, :foo, :bar, self(), :baz) == :ok
      assert PG.join(pg, :foo, :bar, self(), :baz) == {:error, :already_joined}
    end

    test "cleans up entries after process dies", %{pg: pg} do
      {pid, ref} = spawn_monitor(Process, :sleep, [:infinity])
      PG.join(pg, :foo, :bar, pid, :baz)
      assert [_] = PG.members(pg, :foo)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
      assert [] = PG.members(pg, :foo)
    end

    test "pg dies if other linked process dies", %{pg: pg} do
      parent = self()
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(pg, "Supervisor"))
      ref = Process.monitor(pid)

      temp =
        spawn(fn ->
          Process.link(pid)
          send(parent, :continue)
          Process.sleep(:infinity)
        end)

      assert_receive :continue

      Process.exit(temp, :shutdown)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
  end

  describe "leave/2" do
    test "removes entry", %{pg: pg} do
      PG.join(pg, :foo, :bar, self(), :baz)

      assert [_] = PG.members(pg, :foo)
      assert PG.leave(pg, self()) == :ok
      assert [] == PG.members(pg, :foo)
    end

    test "does not remove non members", %{pg: pg} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(pg, "Supervisor"))
      Process.link(pid)

      assert PG.leave(pg, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "leave/4" do
    test "removes single entry", %{pg: pg} do
      PG.join(pg, :foo, :bar, self(), :baz)
      assert [_] = PG.members(pg, :foo)

      assert PG.leave(pg, :foo, :bar, self()) == :ok
      assert [] == PG.members(pg, :foo)
    end

    test "leaves other entries intact", %{pg: pg} do
      PG.join(pg, :foo, :bar, self(), :baz)
      PG.join(pg, :foo, :baar, self(), :baz)
      assert [_, _] = PG.members(pg, :foo)

      assert PG.leave(pg, :foo, :bar, self()) == :ok
      assert [{:baar, :baz}] == PG.members(pg, :foo)
    end

    test "does not remove non members", %{pg: pg} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(pg, "Supervisor"))
      Process.link(pid)

      assert PG.leave(pg, :foo, :bar, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "update/5" do
    test "executes the update if entry is present", %{pg: pg} do
      parent = self()
      PG.join(pg, :foo, :bar, self(), 1)
      assert [{:bar, 1}] == PG.members(pg, :foo)

      update = fn value ->
        send(parent, value)
        value + 1
      end

      assert PG.update(pg, :foo, :bar, self(), update) == :ok
      assert_received 1
      assert [{:bar, 2}] == PG.members(pg, :foo)
    end

    test "does not execute update if entry is absent", %{pg: pg} do
      parent = self()
      update = fn value -> Process.exit(parent, {:unexpected_update, value}) end
      assert PG.update(pg, :foo, :bar, self(), update) == {:error, :not_member}
    end
  end

  describe "replace/5" do
    test "updates value if entry is present", %{pg: pg} do
      PG.join(pg, :foo, :bar, self(), 1)
      assert [{:bar, 1}] == PG.members(pg, :foo)
      assert PG.replace(pg, :foo, :bar, self(), 2) == :ok
      assert [{:bar, 2}] == PG.members(pg, :foo)
    end

    test "does not update value if entry is absent", %{pg: pg} do
      assert PG.replace(pg, :foo, :bar, self(), 2) == {:error, :not_member}
    end
  end
end
