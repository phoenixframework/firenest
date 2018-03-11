defmodule Firenest.PubSubTest do
  use ExUnit.Case, async: true

  alias Firenest.PubSub, as: P
  alias Firenest.Topology, as: T

  setup_all do
    nodes = [:"first@127.0.0.1", :"second@127.0.0.1"]
    pubsub = Firenest.Test.PubSub
    %{start: start} = P.child_spec(name: pubsub, topology: Firenest.Test)
    Firenest.Test.start_link(nodes, start)
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator, pubsub: pubsub}
  end

  setup %{pubsub: pubsub, test: test} do
    topic = Atom.to_string(test)
    :ok = P.subscribe(pubsub, topic)
    {:ok, topic: topic}
  end

  describe "broadcast/3" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, topic: topic} do
      :ok = task_broadcast(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts twice if subscribed twice", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, topic)
      :ok = task_broadcast(pubsub, topic, :hello)
      assert_received :hello
      assert_received :hello
    end

    test "does not broadcast to unsubscribed processes", %{pubsub: pubsub, topic: topic} do
      :ok = P.unsubscribe(pubsub, topic)
      :ok = task_broadcast(pubsub, topic, :hello)
      refute_received :hello
    end

    test "broadcasts to self", %{pubsub: pubsub, topic: topic} do
      :ok = P.broadcast(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, "extra: #{topic}")
      :ok = P.broadcast(pubsub, [topic, "extra: #{topic}"], :hello)
      assert_received :hello
      assert_received :hello
    end

    test "broadcasts and raises on errors", %{pubsub: pubsub, topic: topic} do
      :ok = P.broadcast!(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "is distributed", config do
      %{topology: topology, evaluator: evaluator, pubsub: pubsub, topic: topic} = config

      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        if Process.whereis(unquote(pubsub)) do
          P.broadcast(unquote(pubsub), unquote(topic), {:reply, T.node(unquote(topology))})
        end
      end})

      assert_receive {:reply, :"second@127.0.0.1"}
    end
  end

  describe "broadcast_from/4" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, topic: topic} do
      :ok = task_broadcast_from(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "does not broadcast to self", %{pubsub: pubsub, topic: topic} do
      :ok = P.broadcast_from(pubsub, self(), topic, :hello)
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, "extra: #{topic}")
      task_broadcast_from(pubsub, [topic, "extra: #{topic}"], :hello)
      assert_received :hello
      assert_received :hello
    end

    test "broadcasts and raises on errors", %{pubsub: pubsub, topic: topic} do
      :ok = P.broadcast_from!(pubsub, self(), topic, :hello)
      refute_received :hello
    end

    test "is distributed", config do
      %{topology: topology, evaluator: evaluator, pubsub: pubsub, topic: topic} = config

      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        if Process.whereis(unquote(pubsub)) do
          P.broadcast_from(unquote(pubsub), self(), unquote(topic), {:reply, T.node(unquote(topology))})
        end
      end})

      assert_receive {:reply, :"second@127.0.0.1"}
    end
  end

  describe "local_broadcast/3" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, topic: topic} do
      :ok = task_local_broadcast(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts twice if subscribed twice", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, topic)
      :ok = task_local_broadcast(pubsub, topic, :hello)
      assert_received :hello
      assert_received :hello
    end

    test "does not broadcast to unsubscribed processes", %{pubsub: pubsub, topic: topic} do
      :ok = P.unsubscribe(pubsub, topic)
      :ok = task_local_broadcast(pubsub, topic, :hello)
      refute_received :hello
    end

    test "broadcasts to self", %{pubsub: pubsub, topic: topic} do
      :ok = P.local_broadcast(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, "extra: #{topic}")
      :ok = P.local_broadcast(pubsub, [topic, "extra: #{topic}"], :hello)
      assert_received :hello
      assert_received :hello
    end
  end

  describe "local_broadcast_from/4" do
    test "broadcasts messages to those listening", %{pubsub: pubsub, topic: topic} do
      :ok = task_local_broadcast_from(pubsub, topic, :hello)
      assert_received :hello
      refute_received :hello
    end

    test "does not broadcast to self", %{pubsub: pubsub, topic: topic} do
      :ok = P.local_broadcast_from(pubsub, self(), topic, :hello)
      refute_received :hello
    end

    test "broadcasts on multiple topics", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, "extra: #{topic}")
      task_local_broadcast_from(pubsub, [topic, "extra: #{topic}"], :hello)
      assert_received :hello
      assert_received :hello
    end
  end

  describe "unregister/2" do
    test "unregisters duplicate topics at once", %{pubsub: pubsub, topic: topic} do
      P.subscribe(pubsub, topic)
      :ok = task_broadcast(pubsub, topic, :hello)
      assert_received :hello
      assert_received :hello
      P.unsubscribe(pubsub, topic)
      :ok = task_broadcast(pubsub, topic, :world)
      refute_received :world
    end
  end

  describe "topics/2" do
    test "unregisters duplicate topics at once", %{pubsub: pubsub, topic: topic} do
      assert P.topics(pubsub, self()) == [topic]
      P.unsubscribe(pubsub, topic)
      assert P.topics(pubsub, self()) == []
      P.subscribe(pubsub, "hello")
      P.subscribe(pubsub, "world")
      assert P.topics(pubsub, self()) |> Enum.sort() == ["hello", "world"]
      assert P.topics(pubsub, spawn(fn -> :ok end)) == []
    end
  end

  describe "child_spec/1" do
    test "supports and validates :partitions option", %{topology: topology} do
      Process.flag(:trap_exit, true)
      {:error, _} = start_supervised({P, name: :pubsub_with_partitions,
                                 topology: topology, partitions: 0})
    end

    test "supports custom dispatching", %{topology: topology, topic: topic} do
      {:ok, _} = start_supervised({P, name: :pubsub_with_dispatching,
                         topology: topology, dispatcher: {__MODULE__, :custom_dispatcher}})
      P.subscribe(:pubsub_with_dispatching, topic, :register)
      P.broadcast_from(:pubsub_with_dispatching, self(), topic, :message)
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
