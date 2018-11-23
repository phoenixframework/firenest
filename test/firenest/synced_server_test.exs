defmodule Firenest.SyncedServerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Firenest.SyncedServer, as: S
  alias Firenest.Test.EvalServer

  import Firenest.TestHelpers

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  describe "init/1" do
    test "{:ok, state}", config do
      %{test: test, topology: topology} = config
      fun = fn -> {:ok, 1} end
      assert {:ok, pid} = S.start_link(EvalServer, fun, name: test, topology: topology)
      assert S.call(pid, :state) === 1
    end

    test "{:ok, state, timeout}", config do
      %{test: test, topology: topology} = config
      parent = self()

      fun = fn ->
        timeout = fn ->
          send(parent, 1)
          {:noreply, 2}
        end

        {:ok, timeout, 0}
      end

      assert {:ok, _} = S.start_link(EvalServer, fun, name: test, topology: topology)
      assert_receive 1
    end

    test "{:ok, state, :hibernate}", config do
      %{test: test, topology: topology} = config

      fun = fn ->
        {:ok, 1, :hibernate}
      end

      assert {:ok, pid} = S.start_link(EvalServer, fun, name: test, topology: topology)
      assert_hibernate pid
      assert S.call(pid, :state) === 1
    end

    test ":ignore", config do
      %{test: test, topology: topology} = config
      Process.flag(:trap_exit, true)
      fun = fn -> :ignore end
      assert S.start_link(EvalServer, fun, name: test, topology: topology) === :ignore
      assert Process.whereis(test) === nil
      assert_receive {:EXIT, _, :normal}
    end

    test "{:stop, reason}", config do
      %{test: test, topology: topology} = config
      Process.flag(:trap_exit, true)
      fun = fn -> {:stop, :normal} end

      assert S.start_link(EvalServer, fun, name: test, topology: topology) === {:error, :normal}

      assert Process.whereis(test) === nil
      assert_receive {:EXIT, _, :normal}
    end

    test "exit", config do
      %{test: test, topology: topology} = config
      Process.flag(:trap_exit, true)
      fun = fn -> exit(:normal) end

      assert S.start_link(EvalServer, fun, name: test, topology: topology) === {:error, :normal}

      assert Process.whereis(test) === nil
      assert_receive {:EXIT, _, :normal}
    end

    test "error", config do
      %{test: test, topology: topology} = config
      Process.flag(:trap_exit, true)
      {:current_stacktrace, stack} = Process.info(self(), :current_stacktrace)
      fun = fn -> :erlang.raise(:error, :oops, stack) end

      assert S.start_link(EvalServer, fun, name: test, topology: topology) ===
               {:error, {:oops, stack}}

      assert Process.whereis(test) === nil
      assert_receive {:EXIT, _, {:oops, ^stack}}
    end

    test "throw", config do
      %{test: test, topology: topology} = config
      Process.flag(:trap_exit, true)
      {:current_stacktrace, stack} = Process.info(self(), :current_stacktrace)
      fun = fn -> :erlang.raise(:throw, :oops, stack) end

      assert S.start_link(EvalServer, fun, name: test, topology: topology) ===
               {:error, {{:nocatch, :oops}, stack}}

      assert Process.whereis(test) === nil
      assert_receive {:EXIT, _, {{:nocatch, :oops}, ^stack}}
    end
  end

  describe "handle_call/3" do
    setup %{test: test, topology: topology} do
      {:ok, pid} = S.start_link(EvalServer, 1, name: test, topology: topology)
      {:ok, pid: pid}
    end

    test "{:reply, reply, state}", %{pid: pid} do
      fun = fn _, n -> {:reply, n, n + 1} end
      assert S.call(pid, fun) === 1
      assert S.call(pid, :state) === 2
    end

    test "{:reply, reply, state, timeout}", %{pid: pid} do
      parent = self()

      fun = fn _, n ->
        timeout = fn ->
          send(parent, {:timeout, n})
          {:noreply, n + 1}
        end

        {:reply, n, timeout, 0}
      end

      assert S.call(pid, fun) === 1
      assert_receive {:timeout, 1}
    end

    test "{:noreply, state}", %{pid: pid} do
      fun = fn from, n ->
        S.reply(from, n)
        {:noreply, n + 1}
      end

      assert S.call(pid, fun) === 1
      assert S.call(pid, :state) === 2
    end

    test "{:noreply, state, timeout}", %{pid: pid} do
      parent = self()

      fun = fn from, n ->
        timeout = fn ->
          send(parent, {:timeout, n})
          {:noreply, n + 1}
        end

        S.reply(from, n)
        {:noreply, timeout, 0}
      end

      assert S.call(pid, fun) === 1
      assert_receive {:timeout, 1}
    end

    test "{:reply, reply, state, :hibernate}", %{pid: pid} do
      fun = fn _, n -> {:reply, n, n + 1, :hibernate} end
      assert S.call(pid, fun) === 1

      assert_hibernate pid
      assert S.call(pid, :state) === 2
    end

    test "{:noreply, state, :hibernate}", %{pid: pid} do
      fun = fn from, n ->
        S.reply(from, n)
        {:noreply, n + 1, :hibernate}
      end

      assert S.call(pid, fun) === 1
      assert_hibernate pid
      assert S.call(pid, :state) === 2
    end

    test "{:stop, reason, reply, state}", %{pid: pid} do
      Process.flag(:trap_exit, true)
      parent = self()

      fun = fn _, n ->
        terminate = fn m ->
          send(parent, {:terminate, m})
        end

        {:stop, {:shutdown, terminate}, n, n + 1}
      end

      assert S.call(pid, fun) === 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end

    test "{:stop, reason, state}", %{pid: pid} do
      Process.flag(:trap_exit, true)
      parent = self()

      fun = fn from, n ->
        terminate = fn m ->
          send(parent, {:terminate, m})
        end

        S.reply(from, n)
        {:stop, {:shutdown, terminate}, n + 1}
      end

      assert S.call(pid, fun) === 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "handle_info/2" do
    setup %{test: test, topology: topology} do
      {:ok, pid} = S.start_link(EvalServer, 1, name: test, topology: topology)
      {:ok, pid: pid}
    end

    test "{:noreply, state}", %{pid: pid} do
      parent = self()

      fun = fn :info, n ->
        send(parent, n)
        {:noreply, n + 1}
      end

      send(pid, fun)
      assert_receive 1
      assert S.call(pid, :state) === 2
    end

    test "{:noreply, state, timeout}", %{pid: pid} do
      parent = self()

      fun = fn :info, n ->
        timeout = fn ->
          send(parent, {:timeout, n + 1})
          {:noreply, n + 1}
        end

        send(parent, n)
        {:noreply, timeout, 0}
      end

      send(pid, fun)
      assert_receive 1
      assert_receive {:timeout, 2}
    end

    test "{:noreply, state, :hibernate}", %{pid: pid} do
      parent = self()

      fun = fn :info, n ->
        send(parent, n)
        {:noreply, n + 1, :hibernate}
      end

      send(pid, fun)
      assert_receive 1
      assert_hibernate pid
      assert S.call(pid, :state) === 2
    end

    test "{:stop, reason, state}", %{pid: pid} do
      Process.flag(:trap_exit, true)
      parent = self()

      fun = fn :info, n ->
        terminate = fn m ->
          send(parent, {:terminate, m})
        end

        send(parent, n)
        {:stop, {:shutdown, terminate}, n + 1}
      end

      send(pid, fun)
      assert_receive 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end

    test "terminate exit({:shutdown, _}", %{pid: pid} do
      Process.flag(:trap_exit, true)
      parent = self()

      fun = fn :info, n ->
        format_status = fn m ->
          send(parent, {:format_status, m})
          m + 1
        end

        terminate = fn m ->
          send(parent, {:terminate, m})
          Process.put(:format_status, format_status)
          exit({:shutdown, :terminate})
        end

        send(parent, n)
        {:stop, {:shutdown, terminate}, n + 1}
      end

      assert capture_io(:user, fn ->
               send(pid, fun)
               assert_receive 1
               assert_receive {:terminate, 2}
               assert_receive {:format_status, 2}
               assert_receive {:EXIT, ^pid, {:shutdown, :terminate}}
               Logger.flush()
             end) =~ ~r"error.*GenServer.*\(stop\) shutdown: :terminate.*State: 3"sm
    end
  end
end
