defmodule Firenest.ReplicatedStateTest do
  use ExUnit.Case, async: true

  alias Firenest.Topology, as: T
  alias Firenest.ReplicatedState, as: R

  import Firenest.TestHelpers

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  setup %{test: test, topology: topology} do
    assert {:ok, _} = start_supervised({R, name: test, topology: topology})
    {:ok, server: test}
  end

  describe "join/5" do
    test "adds process", %{server: server} do
      assert R.join(server, :foo, self(), :baz) == :ok
      assert [:baz] == R.members(server, :foo)
    end

    test "rejects double joins", %{server: server} do
      assert R.join(server, :foo, self(), :baz) == :ok
      assert R.join(server, :foo, self(), :baz) == {:error, :already_joined}
    end

    test "cleans up entries after process dies", %{server: server} do
      {pid, ref} = spawn_monitor(Process, :sleep, [:infinity])
      R.join(server, :foo, pid, :baz)
      assert [_] = R.members(server, :foo)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
      assert [] = R.members(server, :foo)
    end

    test "server dies if other linked process dies", %{server: server} do
      parent = self()
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(server, "Supervisor"))
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
    test "removes entry", %{server: server} do
      R.join(server, :foo, self(), :baz)

      assert [_] = R.members(server, :foo)
      assert R.leave(server, self()) == :ok
      assert [] == R.members(server, :foo)
    end

    test "does not remove non members", %{server: server} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(server, "Supervisor"))
      Process.link(pid)

      assert R.leave(server, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "leave/4" do
    test "removes single entry", %{server: server} do
      R.join(server, :foo, self(), :baz)
      assert [_] = R.members(server, :foo)

      assert R.leave(server, :foo, self()) == :ok
      assert [] == R.members(server, :foo)
    end

    test "leaves other entries intact", %{server: server} do
      pid = spawn_link(fn -> Process.sleep(:infinity) end)
      R.join(server, :foo, self(), :baz)
      R.join(server, :foo, pid, :baaz)
      assert [_, _] = R.members(server, :foo)

      assert R.leave(server, :foo, self()) == :ok
      assert [:baaz] == R.members(server, :foo)
    end

    test "does not remove non members", %{server: server} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(server, "Supervisor"))
      Process.link(pid)

      assert R.leave(server, :foo, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "update/5" do
    test "executes the update if entry is present", %{server: server} do
      parent = self()
      R.join(server, :foo, self(), 1)
      assert [1] == R.members(server, :foo)

      update = fn value ->
        send(parent, value)
        value + 1
      end

      assert R.update(server, :foo, self(), update) == :ok
      assert_received 1
      assert [2] == R.members(server, :foo)
    end

    test "does not execute update if entry is absent", %{server: server} do
      parent = self()
      update = fn value -> Process.exit(parent, {:unexpected_update, value}) end
      assert R.update(server, :foo, self(), update) == {:error, :not_member}
    end
  end

  describe "replace/5" do
    test "updates value if entry is present", %{server: server} do
      R.join(server, :foo, self(), 1)
      assert [1] == R.members(server, :foo)
      assert R.replace(server, :foo, self(), 2) == :ok
      assert [2] == R.members(server, :foo)
    end

    test "does not update value if entry is absent", %{server: server} do
      assert R.replace(server, :foo, self(), 2) == {:error, :not_member}
    end
  end

  defmodule Distributed do
    # We modify test topology, it can't be async
    use ExUnit.Case

    setup_all do
      wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end)
      nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
      topology = Firenest.Test
      server = Firenest.Test.ReplicatedServer
      %{start: start} = R.child_spec(name: server, topology: topology)
      Firenest.Test.start_link(nodes, start)
      nodes = for {name, _} = ref <- T.nodes(topology), name in nodes, do: ref

      {:ok, topology: topology, evaluator: Firenest.Test.Evaluator, nodes: nodes, server: server}
    end

    test "remote join is propagated", config do
      %{server: server, test: test, nodes: [second | _]} = config

      quote do
        spawn(fn ->
          :ok = R.join(unquote(server), unquote(test), self(), :baz)
          :timer.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      wait_until(fn -> R.members(server, test) == [:baz] end)
    end

    test "propages changes when nodes were disconnected", config do
      %{topology: topology, server: server, test: test, nodes: [second, third]} = config
      Process.register(self(), test)

      quote do
        spawn(fn ->
          Process.register(self(), unquote(test))
          :ok = R.join(unquote(server), unquote(test), self(), :baz)
          Process.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      quote(do: R.members(unquote(server), unquote(test)) == [:baz])
      |> await_on_node(third, config)

      quote do
        T.disconnect(unquote(topology), elem(unquote(third), 0))
        pid = Process.whereis(unquote(test))
        :ok = R.leave(unquote(server), unquote(test), pid)

        spawn(fn ->
          :ok = R.join(unquote(server), unquote(test), self(), :bar)
          Process.sleep(:infinity)
        end)
      end
      |> eval_on_node(second, config)

      quote(do: R.members(unquote(server), unquote(test)) == [])
      |> await_on_node(third, config)

      quote(do: T.connect(unquote(topology), elem(unquote(third), 0)))
      |> eval_on_node(second, config)

      quote(do: R.members(unquote(server), unquote(test)) == [:bar])
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
