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
end
