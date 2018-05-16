defmodule Firenest.PGTest do
  use ExUnit.Case, async: true

  alias Firenest.PG

  setup_all do
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  setup %{test: test, topology: topology} do
    start_supervised!({PG, name: test, topology: topology})
    {:ok, pg: test}
  end

  test "tracks processes", %{pg: pg} do
    PG.track(pg, self(), :foo, :bar, :baz)
    assert [{:bar, :baz}] == PG.list(pg, :foo)
  end

  test "processes are clened up when they die", %{pg: pg} do
    {pid, ref} = spawn_monitor(Process, :sleep, [:infinity])
    PG.track(pg, pid, :foo, :bar, :baz)
    assert [_] = PG.list(pg, :foo)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
    assert [] = PG.list(pg, :foo)
  end

  test "pg dies if linked, untracked process terminates", %{pg: pg} do
    parent = self()
    [{_, pid, _, _}] = Supervisor.which_children(Module.concat(pg, "Supervisor"))
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

  test "untrack/2", %{pg: pg} do
    PG.track(pg, self(), :foo, :bar, :baz)
    assert [_] = PG.list(pg, :foo)
    PG.untrack(pg, self())
    assert [] == PG.list(pg, :foo)
  end

  test "untrack/4", %{pg: pg} do
    PG.track(pg, self(), :foo, :bar, :baz)
    assert [_] = PG.list(pg, :foo)
    PG.untrack(pg, self(), :foo, :bar)
    assert [] == PG.list(pg, :foo)
  end
end
