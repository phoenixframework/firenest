defmodule Firenest.TopologyTest do
  @moduledoc """
  Tests for the topology API.
  """

  # Must be sync because we are changing the topology.
  use ExUnit.Case
  alias Firenest.Topology, as: T

  setup %{test: test} do
    # Register the current process to receive topology messages.
    Process.register(self(), test)
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  describe "node/1" do
    test "returns the current node name", %{topology: topology} do
      assert T.node(topology) == :"first@127.0.0.1"
    end
  end

  describe "nodes/1" do
    test "returns all connected nodes except self", %{topology: topology} do
      assert T.nodes(topology) |> Enum.sort ==
             [:"second@127.0.0.1", :"third@127.0.0.1"]
    end
  end

  describe "send/4" do
    @describetag :broadcast

    test "messages name in the given node", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      assert T.send(topology, :"third@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end}) == :ok

      assert_receive {:reply, :"third@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"first@127.0.0.1"}
    end

    test "messages name in the current node", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      assert T.send(topology, :"first@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end}) == :ok

      assert_receive {:reply, :"first@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"third@127.0.0.1"}
    end

    test "returns error when messaging unknown node", config do
      %{topology: topology, evaluator: evaluator} = config
      assert T.send(topology, :"unknown@127.0.0.1", evaluator, :oops) ==
             {:error, :noconnection}
    end
  end

  describe "broadcast/3" do
    @describetag :broadcast

    test "messages name in all known nodes except self", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      # We broadcast a message to the evaluator in all nodes
      # and then evaluate code that broadcasts a message back
      # to the test process.
      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        T.broadcast(unquote(topology), unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"third@127.0.0.1"}
      assert_receive {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"first@127.0.0.1"}
    end
  end

  describe "connection" do
    @describetag :connection

    @node :"subscribe@127.0.0.1"
    test "may be set and managed explicitly", %{topology: topology} do
      ref = T.subscribe(topology, self())
      assert is_reference(ref)

      # No node yet
      refute T.disconnect(topology, @node)
      refute_received {:nodedown, @node}

      # Start the node but not firenest
      Firenest.Test.spawn_nodes([@node])
      refute_received {:nodeup, @node}
      refute @node in T.nodes(topology)

      # Finally start firenest
      Firenest.Test.start_firenest([@node], adapter: Firenest.Topology.Erlang)
      assert_receive {:nodeup, @node}
      assert @node in T.nodes(topology)

      # Connect should still return true
      assert T.connect(topology, @node)

      # Now let's diconnect
      assert T.disconnect(topology, @node)
      assert_receive {:nodedown, @node}
      refute @node in T.nodes(topology)

      # And we can't connect it back because it is permanently down
      refute T.connect(topology, @node)
      refute_received {:nodeup, @node}
    after
      T.disconnect(topology, @node)
    end
  end

  describe "monitor" do
    @describetag :monitor

    test "supports local names", %{topology: topology} do
      {:ok, pid} = Agent.start(fn -> %{} end, name: :local_agent)
      ref = T.monitor(topology, :"first@127.0.0.1", :local_agent)
      refute_received {:DOWN, ^ref, _, _, _}
      Process.exit(pid, {:shutdown, :custom_reason})
      assert_receive {:DOWN, ^ref, :process, {:local_agent, :"first@127.0.0.1"}, {:shutdown, :custom_reason}}
    end

    test "returns noproc for unknown local names", %{topology: topology} do
      ref = T.monitor(topology, :"first@127.0.0.1", :local_agent)
      assert_receive {:DOWN, ^ref, :process, {:local_agent, :"first@127.0.0.1"}, :noproc}
    end

    test "returns no connection for unknown nodes", %{topology: topology} do
      ref = T.monitor(topology, :"unknown@127.0.0.1", :local_agent)
      assert_receive {:DOWN, ^ref, :process, {:local_agent, :"unknown@127.0.0.1"}, :noconnection}
    end

    test "supports remote names", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
        {:ok, _} = Agent.start(fn -> %{} end, name: :topology_agent)
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), :done)
      end})

      assert_receive :done
      ref = T.monitor(topology, :"second@127.0.0.1", :topology_agent)
      refute_received {:DOWN, ^ref, _, _, _}

      T.send(topology, :"second@127.0.0.1", evaluator, {:eval_quoted, quote do
        Process.exit(Process.whereis(:topology_agent), {:shutdown, :custom_reason})
      end})

      assert_receive {:DOWN, ^ref, :process, {:topology_agent, :"second@127.0.0.1"}, reason} when
                     reason == :noproc or reason == {:shutdown, :custom_reason}
    end

    test "returns noproc for unknown remote names", %{topology: topology} do
      ref = T.monitor(topology, :"second@127.0.0.1", :unknown_agent)
      assert_receive {:DOWN, ^ref, :process, {:unknown_agent, :"second@127.0.0.1"}, :noproc}
    end

    @node :"topology@127.0.0.1"
    test "returns noconnection on remote disconnection", config do
      %{topology: topology, evaluator: evaluator, test: test} = config
      T.subscribe(topology, self())
      Firenest.Test.spawn_nodes([@node])
      Firenest.Test.start_firenest([@node], adapter: Firenest.Topology.Erlang)
      assert_receive {:nodeup, @node}

      T.send(topology, @node, evaluator, {:eval_quoted, quote do
        {:ok, _} = Agent.start(fn -> %{} end, name: :topology_agent)
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), :done)
      end})

      assert_receive :done
      ref = T.monitor(topology, @node, :topology_agent)

      assert T.disconnect(topology, @node)
      assert_receive {:nodedown, @node}
      assert_receive {:DOWN, ^ref, :process, {:topology_agent, @node}, :noconnection}
    after
      T.disconnect(config.topology, @node)
    end
  end
end
