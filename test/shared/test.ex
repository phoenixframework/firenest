defmodule Firenest.Test do
  @moduledoc """
  A Firenest topology used for testing.

  The code in this module must be compiled as part of Mix
  compilation step as it requires .beam artifacts for the
  cluster setup.
  """

  require Logger

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
  Sets up the current node as the boot server.
  """
  def start_boot_server(node) do
    case :net_kernel.start([node]) do
      {:ok, _} ->
        # Allow spawned nodes to fetch all code from this node
        :erl_boot_server.start([])
        {:ok, ipv4} = :inet.parse_ipv4_address('127.0.0.1')
        :erl_boot_server.add_slave(ipv4)
        :ok

      {:error, _} ->
        raise "make sure epmd is running before starting the test suite. " <>
                "Running `elixir --sname foo` or `epmd -daemon` once is usually enough."
    end
  end

  @doc """
  Spawns the given nodes.
  """
  def spawn_nodes(children) do
    Enum.map(children, &spawn_node/1)
  end

  @doc """
  Starts firenest on the given nodes.
  """
  def start_firenest(nodes, options) do
    %{start: start} = Firenest.Topology.child_spec([name: Firenest.Test] ++ options)
    start_link(nodes, start)
    start_link(nodes, {Evaluator, :start_link, []})
    multirpc(nodes, Firenest.Topology, :node, [Firenest.Test])
  end

  @doc """
  Starts a process given by module and args on the given nodes.
  """
  def start_link(nodes, mfa) when is_tuple(mfa) do
    multirpc(nodes, __MODULE__, :start_link, [mfa])
    :ok
  end

  @doc """
  Sets up current process as a reporter on the given nodes.

  You can report messages back to the caller using `report/1` function.
  """
  def start_reporter(nodes) do
    parent = self()

    spawn_link(fn ->
      Process.register(self(), __MODULE__.Reporter)
      forward(parent)
    end)

    multirpc(nodes, :slave, :pseudo, [node(), [__MODULE__.Reporter]])
    :ok
  end

  defp forward(parent) do
    receive do
      msg -> send(parent, msg)
    end

    forward(parent)
  end

  @doc """
  Sends a report back to the reporter process configured with `start_reporter/1`.
  """
  def report(msg) do
    if pid = Process.whereis(__MODULE__.Reporter) do
      send(pid, msg)
    else
      raise "reporter process not configured on current node"
    end
  end

  @doc false
  def start_link({module, function, args}) do
    parent = self()

    {:ok, task} =
      Task.start_link(fn ->
        apply(module, function, args)
        send(parent, self())
        Process.sleep(:infinity)
      end)

    receive do
      ^task -> :ok
    end
  end

  defp spawn_node(node_host) do
    with_deadline(30_000, fn ->
      {:ok, node} = :slave.start_link('127.0.0.1', node_name(node_host), inet_loader_args())
      add_code_paths(node)
      transfer_configuration(node)
      ensure_applications_started(node)
      {:ok, node}
    end)
  end

  defp with_deadline(timeout, fun) do
    parent = self()
    ref = make_ref()

    pid =
      spawn_link(fn ->
        receive do
          ^ref -> :ok
        after
          timeout ->
            Logger.error("Deadline for starting slave node reached")
            Process.exit(parent, :kill)
        end
      end)

    try do
      fun.()
    after
      send(pid, ref)
    end
  end

  defp rpc(node, module, function, args) do
    :rpc.block_call(node, module, function, args)
  end

  defp multirpc(nodes, m, f, a) do
    case :rpc.multicall(nodes, m, f, a) do
      {results, []} -> results
      {_, bad} -> raise "rpc #{inspect({m, f, a})} failed on nodes #{inspect(bad)}"
    end
  end

  defp inet_loader_args do
    '-loader inet -hosts 127.0.0.1 -setcookie #{:erlang.get_cookie()}'
  end

  defp add_code_paths(node) do
    rpc(node, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(node) do
    for {app_name, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app_name) do
        rpc(node, Application, :put_env, [app_name, key, val])
      end
    end
  end

  defp ensure_applications_started(node) do
    rpc(node, Application, :ensure_all_started, [:mix])
    rpc(node, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.loaded_applications() do
      rpc(node, Application, :ensure_all_started, [app_name])
    end
  end

  defp node_name(node_host) do
    node_host
    |> to_string
    |> String.split("@")
    |> Enum.at(0)
    |> String.to_atom()
  end
end
