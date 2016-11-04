defmodule Firenest.PubSubTest do
  use ExUnit.Case, async: true

  alias Firenest.PubSub, as: P
  alias Firenest.Topology, as: T

  setup_all do
    nodes = [:"first@127.0.0.1", :"second@127.0.0.1"]
    pubsub = Firenest.Test.PubSub
    Firenest.Test.start_link(nodes, Firenest.PubSub, [pubsub, [topology: Firenest.Test]])
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator, pubsub: pubsub}
  end

  setup %{pubsub: pubsub, test: test} do
    :ok = P.subscribe(pubsub, test)
  end

  describe "broadcast/3" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, test: test} do
      :ok = task_broadcast(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts twice if subscribed twice", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, test)
      :ok = task_broadcast(pubsub, test, :hello)
      assert_received :hello
      assert_received :hello
    end

    test "does not broadcast to unsubscribed processes", %{pubsub: pubsub, test: test} do
      :ok = P.unsubscribe(pubsub, test)
      :ok = task_broadcast(pubsub, test, :hello)
      refute_received :hello
    end

    test "broadcasts to self", %{pubsub: pubsub, test: test} do
      :ok = P.broadcast(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, :"extra: #{test}")
      :ok = P.broadcast(pubsub, [test, :"extra: #{test}"], :hello)
      assert_received :hello
      assert_received :hello
    end

    test "broadcasts and raises on errors", %{pubsub: pubsub, test: test} do
      :ok = P.broadcast!(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "is distributed", config do
      %{topology: topology, evaluator: evaluator, pubsub: pubsub, test: test} = config

      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        P.broadcast(unquote(pubsub), unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"second@127.0.0.1"}
    end
  end

  describe "broadcast_from/4" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, test: test} do
      :ok = task_broadcast_from(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "does not broadcast to self", %{pubsub: pubsub, test: test} do
      :ok = P.broadcast_from(pubsub, self(), test, :hello)
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, :"extra: #{test}")
      task_broadcast_from(pubsub, [test, :"extra: #{test}"], :hello)
      assert_received :hello
      assert_received :hello
    end

    test "broadcasts and raises on errors", %{pubsub: pubsub, test: test} do
      :ok = P.broadcast_from!(pubsub, self(), test, :hello)
      refute_received :hello
    end

    test "is distributed", config do
      %{topology: topology, evaluator: evaluator, pubsub: pubsub, test: test} = config

      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        P.broadcast_from(unquote(pubsub), self(), unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"second@127.0.0.1"}
    end
  end

  describe "local_broadcast/3" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, test: test} do
      :ok = task_local_broadcast(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts twice if subscribed twice", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, test)
      :ok = task_local_broadcast(pubsub, test, :hello)
      assert_received :hello
      assert_received :hello
    end

    test "does not broadcast to unsubscribed processes", %{pubsub: pubsub, test: test} do
      :ok = P.unsubscribe(pubsub, test)
      :ok = task_local_broadcast(pubsub, test, :hello)
      refute_received :hello
    end

    test "broadcasts to self", %{pubsub: pubsub, test: test} do
      :ok = P.local_broadcast(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, :"extra: #{test}")
      :ok = P.local_broadcast(pubsub, [test, :"extra: #{test}"], :hello)
      assert_received :hello
      assert_received :hello
    end
  end

  describe "local_broadcast_from/4" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, test: test} do
      :ok = task_local_broadcast_from(pubsub, test, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "does not broadcast to self", %{pubsub: pubsub, test: test} do
      :ok = P.local_broadcast_from(pubsub, self(), test, :hello)
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, :"extra: #{test}")
      task_local_broadcast_from(pubsub, [test, :"extra: #{test}"], :hello)
      assert_received :hello
      assert_received :hello
    end
  end

  describe "unregister/2" do
    test "unregisters duplicate topics at once", %{pubsub: pubsub, test: test} do
      P.subscribe(pubsub, test)
      :ok = task_broadcast(pubsub, test, :hello)
      assert_received :hello
      assert_received :hello
      P.unsubscribe(pubsub, test)
      :ok = task_broadcast(pubsub, test, :world)
      refute_received :world
    end
  end

  describe "start_link/2" do
    test "supports and validates :partitions option", %{topology: topology} do
      Process.flag(:trap_exit, true)
      {:error, _} = P.start_link(:pubsub_with_partitions, topology: topology, partitions: 0)
    end

    test "supports custom dispatching", %{topology: topology, test: test} do
      P.start_link(:pubsub_with_dispatching, topology: topology,
                                             dispatcher: {__MODULE__, :custom_dispatcher})
      P.subscribe(:pubsub_with_dispatching, test, :register)
      P.broadcast_from(:pubsub_with_dispatching, self(), test, :message)
      assert_received {:custom_dispatcher, :register, pid, :message} when pid == self()
    end
  end

  def custom_dispatcher(entries, from, message) do
    for {pid, value} <- entries, do: send(pid, {:custom_dispatcher, value, from, message})
  end

  defp task_broadcast(pubsub, topic, value) do
    Task.async(fn -> P.broadcast(pubsub, topic, value) end) |> Task.await()
  end

  defp task_broadcast_from(pubsub, topic, value) do
    Task.async(fn -> P.broadcast_from(pubsub, self(), topic, value) end) |> Task.await()
  end

  defp task_local_broadcast(pubsub, topic, value) do
    Task.async(fn -> P.local_broadcast(pubsub, topic, value) end) |> Task.await()
  end

  defp task_local_broadcast_from(pubsub, topic, value) do
    Task.async(fn -> P.local_broadcast_from(pubsub, self(), topic, value) end) |> Task.await()
  end
end
