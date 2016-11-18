defmodule Firenest.MonitorTest do
  use ExUnit.Case

  alias Firenest.Monitor, as: M
  alias Firenest.Topology, as: T

  setup_all do
    nodes = [:"first@127.0.0.1", :"second@127.0.0.1"]
    monitor = Firenest.Test.Monitor
    Firenest.Test.start_link(nodes, M, [Firenest.Test, monitor])
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator, monitor: monitor}
  end

  setup %{test: test} do
    Process.register(self(), test)
    :ok
  end

  test "supports local names", %{monitor: monitor} do
    {:ok, pid} = Agent.start(fn -> %{} end, name: :local_agent)
    ref = M.monitor(monitor, :"first@127.0.0.1", :local_agent)
    refute_received {:DOWN, ^ref, _, _, _}
    Process.exit(pid, {:shutdown, :custom_reason})
    assert_receive {:DOWN, ^ref, :process, {:local_agent, :"first@127.0.0.1"}, reason} when
                   reason == :noproc or reason == {:shutdown, :custom_reason}
  end

  test "returns noproc for unknown local names", %{monitor: monitor} do
    ref = M.monitor(monitor, :"first@127.0.0.1", :local_agent)
    assert_receive {:DOWN, ^ref, :process, {:local_agent, :"first@127.0.0.1"}, :noproc}
  end

  test "returns no connection for unknown nodes", %{monitor: monitor} do
    ref = M.monitor(monitor, :"unknown@127.0.0.1", :local_agent)
    assert_receive {:DOWN, ^ref, :process, {:local_agent, :"unknown@127.0.0.1"}, :noconnection}
  end

  test "supports remote names", config do
    %{topology: topology, evaluator: evaluator, monitor: monitor, test: test} = config

    T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
      {:ok, _} = Agent.start(fn -> %{} end, name: :monitor_agent)
      T.send(unquote(topology), :"first@127.0.0.1", unquote(test), :done)
    end})

    assert_receive :done
    ref = M.monitor(monitor, :"second@127.0.0.1", :monitor_agent)
    refute_received {:DOWN, ^ref, _, _, _}

    T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
      Process.exit(Process.whereis(:monitor_agent), {:shutdown, :custom_reason})
    end})

    assert_receive {:DOWN, ^ref, :process, {:monitor_agent, :"second@127.0.0.1"}, reason} when
                   reason == :noproc or reason == {:shutdown, :custom_reason}
  end

  test "returns noproc for unknown remote names", %{monitor: monitor} do
    ref = M.monitor(monitor, :"second@127.0.0.1", :unknown_agent)
    assert_receive {:DOWN, ^ref, :process, {:unknown_agent, :"second@127.0.0.1"}, :noproc}
  end

  @node :"monitor@127.0.0.1"
  test "returns noconnection on remote disconnection", config do
    %{topology: topology, evaluator: evaluator, test: test, monitor: monitor} = config
    T.subscribe(topology, self())
    Firenest.Test.spawn_nodes([@node])
    Firenest.Test.start_firenest([@node], adapter: Firenest.Topology.Erlang)
    assert_receive {:nodeup, @node}

    T.send(topology, @node, evaluator, {:eval_quoted, quote do
      {:ok, _} = Agent.start(fn -> %{} end, name: :monitor_agent)
      T.send(unquote(topology), :"first@127.0.0.1", unquote(test), :done)
    end})

    assert_receive :done
    ref = M.monitor(monitor, @node, :monitor_agent)

    assert T.disconnect(topology, @node)
    assert_receive {:nodedown, @node}
    assert_receive {:DOWN, ^ref, :process, {:monitor_agent, @node}, :noconnection}
  after
    T.disconnect(config.topology, @node)
  end

  test "garbage collects references when monitoring process is down", config do
    %{topology: topology, evaluator: evaluator, monitor: monitor, test: test} = config

    T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
      {:ok, _} = Agent.start(fn -> %{} end, name: :monitor_agent)
      T.send(unquote(topology), :"first@127.0.0.1", unquote(test), :done)
    end})

    assert_receive :done
    {:ok, pid} = Task.start_link(fn ->
      M.monitor(monitor, :"second@127.0.0.1", :monitor_agent)
    end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, _, _, :normal}

    T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
      T.send(unquote(topology), :"first@127.0.0.1", unquote(test),
             {:state, :sys.get_state(Firenest.Test.Monitor.Remote)})
    end})

    assert_receive {:state, %{refs: map}} when map == %{}
  end
end
