defmodule Firenest.SyncedServer.DistributedTest do
  use ExUnit.Case

  alias Firenest.SyncedServer, as: S
  alias Firenest.Topology, as: T
  alias Firenest.Test.EvalServer

  import Firenest.TestHelpers

  setup_all do
    wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end, 5000)
    nodes = [:"first@127.0.0.1", :"second@127.0.0.1"]
    topology = Firenest.Test
    nodes = for {name, _} = ref <- T.nodes(topology), name in nodes, do: ref
    node = T.node(topology)
    {:ok, topology: topology, evaluator: Firenest.Test.Evaluator, nodes: nodes, node: node}
  end

  setup %{test: test, topology: topology} do
    {:ok, pid} = S.start_link(EvalServer, 1, name: test, topology: topology)
    mfa = &{S, :start_link, [EvalServer, &1, [name: test, topology: topology]]}
    {:ok, mfa: mfa, pid: pid}
  end

  describe "handle_replica/3" do
    test "both ends receive message", %{pid: pid, node: node} = config do
      parent = self()

      fun = fn status, replica ->
        send(parent, {:replica, status, replica, 1})
        {:noreply, 1}
      end

      remote_fun =
        quote do
          {:ok,
           fn status, replica ->
             send(unquote(parent), {:replica, status, replica, 2})
             {:noreply, 2}
           end}
        end

      send(pid, {:state, fun})
      second = start_another(config, remote_fun)

      assert_receive {:replica, {:up, _}, ^second, 1}
      assert_receive {:replica, {:up, _}, ^node, 2}
    end

    test "{:noreply, state}", %{pid: pid} = config do
      parent = self()

      fun = fn status, replica ->
        send(parent, {:replica, status, replica, 1})
        {:noreply, 1}
      end

      send(pid, {:state, fun})
      second = start_another(config)

      assert_receive {:replica, {:up, _}, ^second, 1}
      assert S.call(pid, :state) == 1
    end

    test "{:noreply, state, timeout}", %{pid: pid} = config do
      parent = self()

      fun = fn status, replica ->
        timeout = fn ->
          send(parent, {:timeout, 2})
          {:noreply, 2}
        end

        send(parent, {:replica, status, replica, 1})

        {:noreply, timeout, 0}
      end

      send(pid, {:state, fun})
      second = start_another(config)

      assert_receive {:replica, {:up, _}, ^second, 1}
      assert_receive {:timeout, 2}
      assert S.call(pid, :state) == 2
    end

    test "{:noreply, state, :hibernate}", %{pid: pid} = config do
      parent = self()

      fun = fn status, replica ->
        send(parent, {:replica, status, replica, 1})
        {:noreply, 1, :hibernate}
      end

      send(pid, {:state, fun})
      second = start_another(config)

      assert_hibernate pid
      assert_receive {:replica, {:up, _}, ^second, 1}
      assert S.call(pid, :state) == 1
    end

    test "{:stop, reason, state}", %{pid: pid} = config do
      parent = self()
      Process.flag(:trap_exit, true)

      fun = fn status, replica ->
        terminate = fn m ->
          send(parent, {:terminate, m})
        end

        send(parent, {:replica, status, replica, 1})

        {:stop, {:shutdown, terminate}, 1}
      end

      send(pid, {:state, fun})
      second = start_another(config)

      assert_receive {:replica, {:up, _}, ^second, 1}
      assert_receive {:terminate, 1}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end

    test "down", %{test: test} = config do
      parent = self()

      fun = fn status, replica ->
        handle_remote = fn status, replica ->
          send(parent, {:replica, status, replica, 2})
          {:noreply, 2}
        end

        send(parent, {:replica, status, replica, 1})
        {:noreply, handle_remote}
      end

      send(test, {:state, fun})
      second = start_another(config)

      cmd =
        quote do
          pid = Process.whereis(unquote(test))
          Process.exit(pid, :kill)
        end

      assert send_eval(config, second, cmd) == :ok
      assert_receive {:replica, {:up, _}, ^second, 1}
      assert_receive {:replica, :down, ^second, 2}
      assert S.call(test, :state) == 2
    end
  end

  describe "handle_remote/3" do
    setup config do
      {:ok, second: start_another(config)}
    end

    test "{:noreply, state}", config do
      %{second: second, node: node, test: test, pid: pid} = config
      parent = self()

      cmd =
        quote do
          fun = fn :info, state ->
            handle_remote = fn from, n ->
              send(unquote(parent), {:remote, from, n})
              {:noreply, n + 1}
            end

            S.remote_send(unquote(node), handle_remote)
            {:noreply, state}
          end

          send(unquote(test), fun)
        end

      assert send_eval(config, second, cmd) == :ok
      assert_receive {:remote, ^second, 2}
      assert S.call(pid, :state) == 3
    end

    test "{:noreply, state, timeout}", config do
      %{second: second, test: test, pid: pid} = config
      parent = self()

      cmd =
        quote do
          fun = fn :info, state ->
            handle_remote = fn from, n ->
              timeout = fn ->
                send(unquote(parent), {:timeout, n + 1})
                {:noreply, n + 1}
              end

              send(unquote(parent), {:remote, from, n})
              {:noreply, timeout, 0}
            end

            S.remote_broadcast(handle_remote)
            {:noreply, state}
          end

          send(unquote(test), fun)
        end

      assert send_eval(config, second, cmd) == :ok
      assert_receive {:remote, ^second, 2}
      assert_receive {:timeout, 3}
      assert S.call(pid, :state) == 3
    end

    test "{:noreply, state, :hibernate}", config do
      %{second: second, test: test, pid: pid} = config
      parent = self()

      cmd =
        quote do
          fun = fn :info, state ->
            handle_remote = fn from, n ->
              send(unquote(parent), {:remote, from, n})
              {:noreply, n + 1, :hibernate}
            end

            S.remote_broadcast(handle_remote)
            {:noreply, state}
          end

          send(unquote(test), fun)
        end

      assert send_eval(config, second, cmd) == :ok
      assert_receive {:remote, ^second, 2}
      assert_hibernate pid
      assert S.call(pid, :state) == 3
    end

    test "{:stop, reason, state}", config do
      %{second: second, node: node, test: test, pid: pid} = config
      parent = self()
      Process.flag(:trap_exit, true)

      cmd =
        quote do
          fun = fn :info, state ->
            handle_remote = fn from, n ->
              terminate = fn m ->
                send(unquote(parent), {:terminate, m})
              end

              send(unquote(parent), {:remote, from, n})
              {:stop, {:shutdown, terminate}, n + 1}
            end

            S.remote_send(unquote(node), handle_remote)
            {:noreply, state}
          end

          send(unquote(test), fun)
        end

      assert send_eval(config, second, cmd) == :ok
      assert_receive {:remote, ^second, 2}
      assert_receive {:terminate, 3}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  defp start_another(config) do
    start_another(config, quote(do: {:ok, fn _, _ -> {:noreply, 1} end}))
  end

  defp start_another(config, initial_state) do
    %{mfa: mfa, nodes: [second | _]} = config

    Firenest.Test.start_link([elem(second, 0)], mfa.({:eval, initial_state}))
    second
  end

  defp send_eval(config, to, cmd) do
    %{evaluator: evaluator, topology: topology} = config
    T.send(topology, to, evaluator, {:eval_quoted, cmd})
  end
end
