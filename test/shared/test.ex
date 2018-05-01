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
  end

  @doc """
  Starts a process given by module and args on the given nodes.
  """
  def start_link(nodes, mfa) when is_tuple(mfa) do
    case :rpc.multicall(nodes, __MODULE__, :start_link, [mfa]) do
      {_, []} -> :ok
      {_, bad} -> raise "starting #{inspect(mfa)} in cluster failed on nodes #{inspect(bad)}"
    end

    :ok
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
    token = start_deadline(30_000)
    {:ok, node} = :slave.start_link('127.0.0.1', node_name(node_host), inet_loader_args())
    add_code_paths(node)
    transfer_configuration(node)
    ensure_applications_started(node)
    cancel_deadline(token)
    {:ok, node}
  end

  defp start_deadline(timeout) do
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

    {pid, ref}
  end

  defp cancel_deadline({pid, ref}), do: send(pid, ref)

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
