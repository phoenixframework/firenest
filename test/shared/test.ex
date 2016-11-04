defmodule Firenest.Test do
  @moduledoc """
  A Firenest topology used for testing.

  The code in this module must be compiled as part of Mix
  compilation step as it requires .beam artifacts for the
  cluster setup.
  """

  defmodule Evaluator do
    @moduledoc false

    use GenServer

    def start_link do
      GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_info({:eval_quoted, quoted}, state) do
      Code.eval_quoted(quoted)
      {:noreply, state}
    end
    def handle_info(_, state) do
      {:noreply, state}
    end
  end

  @doc """
  Starts a Firenest topology named `Firenest.Test` on the given nodes.

  It also starts a process named `Firenest.Test.Evaluator` that
  evaluates messages in the format of `{:eval_quoted, quoted}`.
  """
  def spawn([head | tail] = nodes, options) do
    # Turn node into a distributed node with the given long name
    case :net_kernel.start([head]) do
      {:ok, _} ->
        # Allow spawned nodes to fetch all code from this node
        :erl_boot_server.start([])
        {:ok, ipv4} = :inet.parse_ipv4_address('127.0.0.1')
        :erl_boot_server.add_slave(ipv4)

        tail
        |> Enum.map(&Task.async(fn -> spawn_node(&1) end))
        |> Enum.map(&Task.await(&1, 30_000))

        start_link(nodes, Firenest.Topology, [Firenest.Test, options])
        start_link(nodes, Evaluator, [])

      {:error, _} ->
        raise "make sure epmd is running before starting the test suite. " <>
              "Running `elixir --sname foo` or `epmd -daemon` is usually enough."
    end
  end

  @doc """
  Starts a process on the given topology nodes.
  """
  def start_link(nodes, module, args) do
    case :rpc.multicall(nodes, __MODULE__, :start_link, [module, args]) do
      {_, []}  -> :ok
      {_, bad} -> raise "starting #{inspect module} in cluster failed on nodes #{inspect bad}"
    end

    :ok
  end

  @doc false
  def start_link(module, args) do
    parent = self()

    {:ok, task} = Task.start_link(fn ->
      apply(module, :start_link, args)
      send(parent, self())
      Process.sleep(:infinity)
    end)

    receive do
      ^task -> :ok
    end
  end

  defp spawn_node(node_host) do
    {:ok, node} = :slave.start('127.0.0.1', node_name(node_host), inet_loader_args())
    add_code_paths(node)
    transfer_configuration(node)
    ensure_applications_started(node)
    {:ok, node}
  end

  defp rpc(node, module, function, args) do
    :rpc.block_call(node, module, function, args)
  end

  defp inet_loader_args do
    '-loader inet -hosts 127.0.0.1 -setcookie #{:erlang.get_cookie()}'
  end

  defp add_code_paths(node) do
    rpc(node, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(node) do
    for {app_name, _, _} <- Application.loaded_applications do
      for {key, val} <- Application.get_all_env(app_name) do
        rpc(node, Application, :put_env, [app_name, key, val])
      end
    end
  end

  defp ensure_applications_started(node) do
    rpc(node, Application, :ensure_all_started, [:mix])
    rpc(node, Mix, :env, [Mix.env()])
    for {app_name, _, _} <- Application.loaded_applications do
      rpc(node, Application, :ensure_all_started, [app_name])
    end
  end

  defp node_name(node_host) do
    node_host
    |> to_string
    |> String.split("@")
    |> Enum.at(0)
    |> String.to_atom
  end
end
