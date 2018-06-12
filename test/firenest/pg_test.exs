defmodule Firenest.PGTest do
  use ExUnit.Case, async: true

  alias Firenest.Topology, as: T
  alias Firenest.PG

  import Firenest.TestHelpers

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  setup %{test: test, topology: topology} do
    assert {:ok, _} = start_supervised({PG, name: test, topology: topology})
    {:ok, pg: test}
  end

  describe "join/5" do
    test "adds process", %{pg: pg} do
      assert PG.join(pg, :foo, self(), :baz) == :ok
      assert [:baz] == PG.members(pg, :foo)
    end

    test "rejects double joins", %{pg: pg} do
      assert PG.join(pg, :foo, self(), :baz) == :ok
      assert PG.join(pg, :foo, self(), :baz) == {:error, :already_joined}
    end

    test "cleans up entries after process dies", %{pg: pg} do
      {pid, ref} = spawn_monitor(Process, :sleep, [:infinity])
      PG.join(pg, :foo, pid, :baz)
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
      PG.join(pg, :foo, self(), :baz)

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
      PG.join(pg, :foo, self(), :baz)
      assert [_] = PG.members(pg, :foo)

      assert PG.leave(pg, :foo, self()) == :ok
      assert [] == PG.members(pg, :foo)
    end

    test "leaves other entries intact", %{pg: pg} do
      pid = spawn_link(fn -> Process.sleep(:infinity) end)
      PG.join(pg, :foo, self(), :baz)
      PG.join(pg, :foo, pid, :baaz)
      assert [_, _] = PG.members(pg, :foo)

      assert PG.leave(pg, :foo, self()) == :ok
      assert [:baaz] == PG.members(pg, :foo)
    end

    test "does not remove non members", %{pg: pg} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(pg, "Supervisor"))
      Process.link(pid)

      assert PG.leave(pg, :foo, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "update/5" do
    test "executes the update if entry is present", %{pg: pg} do
      parent = self()
      PG.join(pg, :foo, self(), 1)
      assert [1] == PG.members(pg, :foo)

      update = fn value ->
        send(parent, value)
        value + 1
      end

      assert PG.update(pg, :foo, self(), update) == :ok
      assert_received 1
      assert [2] == PG.members(pg, :foo)
    end

    test "does not execute update if entry is absent", %{pg: pg} do
      parent = self()
      update = fn value -> Process.exit(parent, {:unexpected_update, value}) end
      assert PG.update(pg, :foo, self(), update) == {:error, :not_member}
    end
  end

  describe "replace/5" do
    test "updates value if entry is present", %{pg: pg} do
      PG.join(pg, :foo, self(), 1)
      assert [1] == PG.members(pg, :foo)
      assert PG.replace(pg, :foo, self(), 2) == :ok
      assert [2] == PG.members(pg, :foo)
    end

    test "does not update value if entry is absent", %{pg: pg} do
      assert PG.replace(pg, :foo, self(), 2) == {:error, :not_member}
    end
  end

  defmodule Distributed do
    # We modify test topology, it can't be async
    use ExUnit.Case

    setup_all do
      wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end)
      nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
      topology = Firenest.Test
      pg = Firenest.Test.PG
      %{start: start} = PG.child_spec(name: pg, topology: topology)
      Firenest.Test.start_link(nodes, start)
      nodes = for {name, _} = ref <- T.nodes(topology), name in nodes, do: ref

      {:ok, topology: topology, evaluator: Firenest.Test.Evaluator, nodes: nodes, pg: pg}
    end

    setup %{test: test} do
      {:ok, group: test}
    end

    test "remote join is propagated", config do
      %{pg: pg, group: group, nodes: [second | _]} = config

      quote do
        spawn(fn ->
          :ok = PG.join(unquote(pg), unquote(group), self(), :baz)
          :timer.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      wait_until(fn -> PG.members(pg, group) == [:baz] end)
    end

    test "propages changes when nodes were disconnected", config do
      %{topology: topology, pg: pg, test: test, nodes: [second, third]} = config
      Process.register(self(), test)

      quote do
        spawn(fn ->
          Process.register(self(), unquote(test))
          :ok = PG.join(unquote(pg), unquote(test), self(), :baz)
          Process.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      quote(do: PG.members(unquote(pg), unquote(test)) == [:baz])
      |> await_on_node(third, config)

      quote do
        T.disconnect(unquote(topology), elem(unquote(third), 0))
        pid = Process.whereis(unquote(test))
        :ok = PG.leave(unquote(pg), unquote(test), pid)

        spawn(fn ->
          :ok = PG.join(unquote(pg), unquote(test), self(), :bar)
          Process.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      quote(do: PG.members(unquote(pg), unquote(test)) == [])
      |> await_on_node(third, config)

      quote(do: T.connect(unquote(topology), elem(unquote(third), 0)))
      |> eval_on_node(second, config)

      quote(do: PG.members(unquote(pg), unquote(test)) == [:bar])
      |> await_on_node(third, config)
    end

    defp eval_on_node(quoted, node, config) do
      %{topology: topology, evaluator: evaluator} = config

      T.send(topology, node, evaluator, {:eval_quoted, quoted})
    end

    defp await_on_node(quoted, node, config) do
      %{topology: topology} = config
      {:registered_name, name} = Process.info(self(), :registered_name)

      quote do
        wait_until(fn -> unquote(quoted) end)
        T.broadcast(unquote(topology), unquote(name), :continue)
      end
      |> eval_on_node(node, config)

      assert_receive :continue
    end
  end
end
