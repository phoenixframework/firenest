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

      T.send(topology, :"third@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"third@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"first@127.0.0.1"}
    end

    test "messages name in the current node", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      T.send(topology, :"first@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"first@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"third@127.0.0.1"}
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
end
