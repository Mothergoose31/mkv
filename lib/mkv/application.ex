defmodule Mkv.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application


  @port 3000

  @impl true
  def start(_type, _args) do

    config = load_config()

    children = [

      {Finch, name: Mkv.Finch},

      Plug.Cowboy.child_spec(scheme: :http, plug: {Mkv.Router, config}, options: [port: config[:port]]),

      {Mkv.Index, config}

    ]

    opts = [strategy: :one_for_one, name: Mkv.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def load_config() do
    %{
      db_path: System.get_env("MKV_DB_PATH", "/tmp/indexdb"),
      volumes: parse_volumes(System.get_env("MKV_VOLUMES", "localhost:3001,localhost:3002,localhost:3003")),
      replicas: String.to_integer(System.get_env("MKV_REPLICAS", "3")),
      port: String.to_integer(System.get_env("MKV_PORT", "3000")),

    }
  end

  defp parse_volumes(nil), do: []
  defp parse_volumes(volume_str) do
    volume_str
    |> String.split(",", trim: true)
    |> Enum.reject(&(&1 == ""))
  end
end
