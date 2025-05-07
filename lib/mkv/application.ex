defmodule Mkv.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    config = load_config()

    # Check command mode
    mode = System.get_env("MKV_MODE", "server")

    case mode do
      "server" ->
        start_server(config)
      "rebuild" ->
        start_rebuild(config)
      "rebalance" ->
        start_rebalance(config)
      _ ->
        # Default to server mode
        start_server(config)
    end
  end

  defp start_server(config) do
    children = [
      {Finch, name: Mkv.Finch},
      Plug.Cowboy.child_spec(scheme: :http, plug: {Mkv.Router, config}, options: [port: config[:port]]),
      {Mkv.Index, config}
    ]

    opts = [strategy: :one_for_one, name: Mkv.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp start_rebuild(config) do
    case Mkv.Operations.rebuild(config) do
      {:ok, stats} ->
        IO.puts("Rebuild completed successfully.")
        IO.puts("Keys indexed: #{stats.successful}")
        IO.puts("Failed: #{stats.failed}")

      {:error, reason} ->
        IO.puts("Rebuild failed: #{inspect(reason)}")
    end


    System.stop(0)
  end

  defp start_rebalance(config) do
    case Mkv.Operations.rebalance(config) do
      :ok ->
        IO.puts("Rebalance completed successfully.")

      {:error, reason} ->
        IO.puts("Rebalance failed: #{inspect(reason)}")
    end


    System.stop(0)
  end

  def load_config() do
    %{
      db_path: System.get_env("MKV_DB_PATH", "/tmp/indexdb"),
      volumes: parse_volumes(System.get_env("MKV_VOLUMES", "localhost:3001,localhost:3002,localhost:3003")),
      replicas: String.to_integer(System.get_env("MKV_REPLICAS", "3")),
      port: String.to_integer(System.get_env("MKV_PORT", "3000")),
      fallback: System.get_env("MKV_FALLBACK"),
      protect: System.get_env("MKV_PROTECT", "false") == "true",
      subvolumes: String.to_integer(System.get_env("MKV_SUBVOLUMES", "10"))
    }
  end

  defp parse_volumes(nil), do: []
  defp parse_volumes(volume_str) do
    volume_str
    |> String.split(",", trim: true)
    |> Enum.reject(&(&1 == ""))
  end
end
