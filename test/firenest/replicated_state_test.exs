defmodule Firenest.ReplicatedStateTest do
  use ExUnit.Case, async: true

  alias Firenest.Topology, as: T
  alias Firenest.ReplicatedState, as: R

  import Firenest.TestHelpers

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  setup %{test: test, topology: topology} do
    opts = [name: test, topology: topology, handler: Firenest.Test.EvalState]
    assert {:ok, _} = start_supervised({R, opts})
    {:ok, server: test}
  end

  describe "put/4" do
    test "adds process", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_put, config})
        {:ok, 1}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, _}

      assert [1] == R.list(server, :foo)
    end

    test "rejects double puts", %{server: server} do
      assert R.put(server, :foo, self(), 1) == :ok
      assert R.put(server, :foo, self(), 2) == {:error, :already_present}
    end

    test "cleans up entries after process dies", %{server: server} do
      {pid, ref} = spawn_monitor(Process, :sleep, [:infinity])
      R.put(server, :foo, pid, 1)
      assert [_] = R.list(server, :foo)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
      assert [] = R.list(server, :foo)
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

    test "immediately deletes with :delete return", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_put, config})

        delete = fn config ->
          send(parent, {:local_delete, config})
        end

        {:ok, delete, :delete}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, _}
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "updates on a timeout with :update_after return", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_put, config})

        update = fn delta, state, config ->
          send(parent, {:local_update, delta, state, config})
          {delta + 1, state + 1}
        end

        {1, 1, {:update_after, update, 50}}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, _}
      refute_received {:local_update, _, _, _}
      assert [1] == R.list(server, :foo)

      assert_receive {:local_update, 1, 1, _}
      assert [2] == R.list(server, :foo)
    end
  end

  describe "delete/2" do
    test "removes entry", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_delete, config})
      end

      R.put(server, :foo, self(), fn _ -> {:ok, fun} end)

      assert [_] = R.list(server, :foo)
      assert R.delete(server, self()) == :ok
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "does not remove non members", %{server: server} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(server, "Supervisor"))
      Process.link(pid)

      assert R.delete(server, self()) == {:error, :not_member}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "delete/3" do
    test "removes single entry", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_delete, config})
      end

      R.put(server, :foo, self(), fn _ -> {:ok, fun} end)
      assert [_] = R.list(server, :foo)

      assert R.delete(server, :foo, self()) == :ok
      assert_received {:local_delete, _}
      assert [] == R.list(server, :foo)
    end

    test "leaves other entries intact", %{server: server} do
      pid = spawn_link(fn -> Process.sleep(:infinity) end)
      R.put(server, :foo, self(), 1)
      R.put(server, :foo, pid, 2)
      assert [_, _] = R.list(server, :foo)

      assert R.delete(server, :foo, self()) == :ok
      assert [2] == R.list(server, :foo)
    end

    test "does not remove non members", %{server: server} do
      [{_, pid, _, _}] = Supervisor.which_children(Module.concat(server, "Supervisor"))
      Process.link(pid)

      assert R.delete(server, :foo, self()) == {:error, :not_present}
      {:links, links} = Process.info(self(), :links)
      assert pid in links
    end
  end

  describe "update/4" do
    test "executes the update if entry is present", %{server: server} do
      parent = self()

      R.put(server, :foo, self(), 1)
      assert [1] = R.list(server, :foo)

      fun = fn delta, state, config ->
        send(parent, {:local_update, delta, state, config})
        {delta + 1, state + 1}
      end

      assert R.update(server, :foo, self(), fun) == :ok
      assert_received {:local_update, 1, 1, _}
      assert [2] = R.list(server, :foo)
    end

    test "does not execute update if entry is absent", %{server: server} do
      parent = self()

      fun = fn delta, state, config ->
        Process.exit(parent, {:unexpected_update, delta, state, config})
        {delta, state}
      end

      assert R.update(server, :foo, self(), fun) == {:error, :not_present}
    end

    test "immediately deletes with :delete return", %{server: server} do
      parent = self()

      R.put(server, :foo, self(), 1)
      assert [1] = R.list(server, :foo)

      fun = fn delta, state, config ->
        send(parent, {:local_update, delta, state, config})

        delete = fn config ->
          send(parent, {:local_delete, config})
        end

        {delta + 1, delete, :delete}
      end

      assert R.update(server, :foo, self(), fun) == :ok
      assert_received {:local_update, 1, 1, _}
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "updates on a timeout with :update_after return", %{server: server} do
      parent = self()

      R.put(server, :foo, self(), 1)
      assert [1] = R.list(server, :foo)

      fun = fn delta, state, config ->
        send(parent, {:local_update, delta, state, config})

        update = fn delta, state, config ->
          send(parent, {:local_update, delta, state, config})
          {delta + 1, state + 1}
        end

        {delta + 1, state + 1, {:update_after, update, 50}}
      end

      assert R.update(server, :foo, self(), fun) == :ok
      assert_received {:local_update, 1, 1, _}
      refute_received {:local_update, _, _, _}
      assert [2] == R.list(server, :foo)

      assert_receive {:local_update, 2, 2, _}
      assert [3] == R.list(server, :foo)
    end
  end

  # defmodule Distributed do
  #   # We modify test topology, it can't be async
  #   use ExUnit.Case

  #   setup_all do
  #     wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end)
  #     nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
  #     topology = Firenest.Test
  #     server = Firenest.Test.ReplicatedServer
  #     %{start: start} = R.child_spec(name: server, topology: topology)
  #     Firenest.Test.start_link(nodes, start)
  #     nodes = for {name, _} = ref <- T.nodes(topology), name in nodes, do: ref

  #     {:ok, topology: topology, evaluator: Firenest.Test.Evaluator, nodes: nodes, server: server}
  #   end

  #   test "remote join is propagated", config do
  #     %{server: server, test: test, nodes: [second | _]} = config

  #     quote do
  #       spawn(fn ->
  #         :ok = R.join(unquote(server), unquote(test), self(), :baz)
  #         :timer.sleep(:infinity)
  #       end)
  #     end
  #     |> eval_on_node(second, config)

  #     wait_until(fn -> R.members(server, test) == [:baz] end)
  #   end

  #   test "propages changes when nodes were disconnected", config do
  #     %{topology: topology, server: server, test: test, nodes: [second, third]} = config
  #     Process.register(self(), test)

  #     quote do
  #       spawn(fn ->
  #         Process.register(self(), unquote(test))
  #         :ok = R.join(unquote(server), unquote(test), self(), :baz)
  #         Process.sleep(:infinity)
  #       end)
  #     end
  #     |> eval_on_node(second, config)

  #     quote(do: R.members(unquote(server), unquote(test)) == [:baz])
  #     |> await_on_node(third, config)

  #     quote do
  #       T.disconnect(unquote(topology), elem(unquote(third), 0))
  #       pid = Process.whereis(unquote(test))
  #       :ok = R.leave(unquote(server), unquote(test), pid)

  #       spawn(fn ->
  #         :ok = R.join(unquote(server), unquote(test), self(), :bar)
  #         Process.sleep(:infinity)
  #       end)
  #     end
  #     |> eval_on_node(second, config)

  #     quote(do: R.members(unquote(server), unquote(test)) == [])
  #     |> await_on_node(third, config)

  #     quote(do: T.connect(unquote(topology), elem(unquote(third), 0)))
  #     |> eval_on_node(second, config)

  #     quote(do: R.members(unquote(server), unquote(test)) == [:bar])
  #     |> await_on_node(third, config)
  #   end

  #   defp eval_on_node(quoted, node, config) do
  #     %{topology: topology, evaluator: evaluator} = config

  #     T.send(topology, node, evaluator, {:eval_quoted, quoted})
  #   end

  #   defp await_on_node(quoted, node, config) do
  #     %{topology: topology} = config
  #     {:registered_name, name} = Process.info(self(), :registered_name)

  #     quote do
  #       wait_until(fn -> unquote(quoted) end)
  #       T.broadcast(unquote(topology), unquote(name), :continue)
  #     end
  #     |> eval_on_node(node, config)

  #     assert_receive :continue
  #   end
  # end
end
