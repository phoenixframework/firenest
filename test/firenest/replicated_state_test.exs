defmodule Firenest.ReplicatedStateTest do
  use ExUnit.Case, async: true

  alias Firenest.ReplicatedState, as: R

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

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})
        {delta + 1, 1}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}

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

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})

        delete = fn config ->
          send(parent, {:local_delete, config})
        end

        {delta + 1, delete, :delete}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "immediately deletes with :delete return and other keys", %{server: server} do
      parent = self()

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})

        delete = fn config ->
          send(parent, {:local_delete, config})
        end

        {delta + 1, delete, :delete}
      end

      assert R.put(server, :bar, self(), 1) == :ok
      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "with :update_after return", %{server: server} do
      parent = self()

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})

        update = fn delta, state, config ->
          send(parent, {:local_update, delta, state, config})
          {delta + 1, state + 1}
        end

        {delta + 1, 1, {:update_after, update, 50}}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}
      refute_received {:local_update, _, _, _}
      assert [1] == R.list(server, :foo)

      assert_receive {:local_update, 1, 1, _}
      assert [2] == R.list(server, :foo)
    end

    test "with :update_after return that deletes immediately", %{server: server} do
      parent = self()

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})

        update = fn delta, state, config ->
          send(parent, {:local_update, delta, state, config})

          delete = fn config ->
            send(parent, {:local_delete, config})
          end

          {delta + 1, delete, :delete}
        end

        {delta + 1, 1, {:update_after, update, 50}}
      end

      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}
      refute_received {:local_update, _, _, _}
      assert [1] == R.list(server, :foo)

      assert_receive {:local_update, 1, 1, _}
      assert_received {:local_delete, _}
      assert [] == R.list(server, :foo)
    end

    test "with :update_after return that deletes immediately with other keys", %{server: server} do
      parent = self()

      fun = fn delta, config ->
        send(parent, {:local_put, delta, config})

        update = fn delta, state, config ->
          send(parent, {:local_update, delta, state, config})

          delete = fn config ->
            send(parent, {:local_delete, config})
          end

          {delta + 1, delete, :delete}
        end

        {delta + 1, 1, {:update_after, update, 50}}
      end

      assert R.put(server, :bar, self(), 1) == :ok
      assert R.put(server, :foo, self(), fun) == :ok
      assert_received {:local_put, 0, _}
      refute_received {:local_update, _, _, _}
      assert [1] == R.list(server, :foo)

      assert_receive {:local_update, 1, 1, _}
      assert_received {:local_delete, _}
      assert [] == R.list(server, :foo)
      assert [_] = R.list(server, :bar)
    end
  end

  describe "delete/2" do
    test "removes entry", %{server: server} do
      parent = self()

      fun = fn config ->
        send(parent, {:local_delete, config})
      end

      R.put(server, :foo, self(), fn delta, _ -> {delta, fun} end)

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

      R.put(server, :foo, self(), fn delta, _ -> {delta, fun} end)
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

    test "leaves other keys for same process intact", %{server: server} do
      R.put(server, :foo, self(), 1)
      R.put(server, :bar, self(), 2)
      assert [_] = R.list(server, :foo)
      assert [_] = R.list(server, :bar)

      assert R.delete(server, :foo, self()) == :ok
      assert [] = R.list(server, :foo)
      assert [_] = R.list(server, :bar)
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
      assert_received {:local_update, 0, 1, _}
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
      assert_received {:local_update, 0, 1, _}
      assert_received {:local_delete, _}

      assert [] == R.list(server, :foo)
    end

    test "immediately deletes with :delete return and other keys", %{server: server} do
      parent = self()

      R.put(server, :bar, self(), 1)
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
      assert_received {:local_update, 0, 1, _}
      assert_received {:local_delete, _}

      assert [] = R.list(server, :foo)
      assert [_] = R.list(server, :bar)
    end

    test "with :update_after return", %{server: server} do
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
      assert_received {:local_update, 0, 1, _}
      refute_received {:local_update, _, _, _}
      assert [2] == R.list(server, :foo)

      assert_receive {:local_update, 1, 2, _}
      assert [3] == R.list(server, :foo)
    end
  end
end
