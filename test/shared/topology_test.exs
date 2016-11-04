defmodule Firenest.TopologyTest do
  @moduledoc """
  Tests for the topology API.
  """

  use ExUnit.Case, async: true
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

  describe "broadcast/3" do
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
