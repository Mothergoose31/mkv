defmodule Mkv.Router do
  use Plug.Router
  import Plug.Conn
  require Logger

  plug :match
  plug :fetch_query_params
  plug :dispatch

  get "/" do
    case conn.params["unlinked"] do
      nil ->
        send_resp(conn, 404, "Not Found")
      _ ->
        case Mkv.Index.list_unlinked() do
          {:ok, keys} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{keys: keys}))

          {:error, reason} ->
            Logger.error("Failed to list unlinked keys: #{inspect(reason)}")
            send_resp(conn, 500, "Internal Server Error")
        end
    end
  end

  get "/:key_or_prefix" do
    key_or_prefix = conn.params["key_or_prefix"]

    case conn.params["list"] do
      nil ->

        case Mkv.Index.get(key_or_prefix) do
          {:ok, {volumes, path_on_volume}} ->
            redirect_url = "http://" <> List.first(volumes) <> path_on_volume
            conn
            |> put_resp_header("location", redirect_url)
            |> send_resp(302, "Found: Redirecting to #{redirect_url}")

          :not_found ->
            send_resp(conn, 404, "Not Found")

          {:error, reason} ->
            Logger.error("Error getting key '#{key_or_prefix}': #{inspect(reason)}")
            send_resp(conn, 500, "Internal Server Error")
        end

      _ ->
        case Mkv.Index.list_prefix(key_or_prefix) do
          {:ok, keys} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{keys: keys}))

          {:error, reason} ->
            Logger.error("Failed to list keys with prefix '#{key_or_prefix}': #{inspect(reason)}")
            send_resp(conn, 500, "Internal Server Error")
        end
    end
  end

  put "/:key" do
    key = conn.params["key"]


    {:ok, body, conn} = read_body(conn)

    all_volumes = Mkv.Application.load_config()[:volumes]
    replicas = Mkv.Application.load_config()[:replicas]
    target_volumes = Enum.take(all_volumes, replicas)

    path_on_volume = "/data/" <> key
    results =
      target_volumes
      |> Task.async_stream(&put_to_volume(&1, path_on_volume, body), ordered: false, timeout: 10000)
      |> Enum.to_list()

    successful_puts =
      Enum.filter(results, fn
        {:ok, {:ok, %{status: status}}} when status >= 200 and status < 300 -> true
        _ -> false
      end)

    if length(successful_puts) >= replicas do
      value_location_info = {target_volumes, path_on_volume}
      case Mkv.Index.put(key, value_location_info) do
        :ok ->
          send_resp(conn, 201, "Created")
        {:error, reason} ->
          Logger.error("Failed to update index for key '#{key}': #{inspect(reason)}")
          send_resp(conn, 500, "Internal Server Error (Index Update Failed)")
      end
    else
      Logger.error("Failed to write enough replicas for key '#{key}'. Success: #{length(successful_puts)}, Needed: #{replicas}")
      send_resp(conn, 503, "Service Unavailable (Failed to write replicas)")
    end
  end

  delete "/:key" do
    key = conn.params["key"]

    case Mkv.Index.get(key) do
      {:ok, {volumes, path_on_volume}} ->
        _results =
          volumes
          |> Task.async_stream(&delete_from_volume(&1, path_on_volume), ordered: false, timeout: 10000)
          |> Enum.to_list()

        case Mkv.Index.delete(key) do
          :ok ->
            send_resp(conn, 204, "")
          {:error, reason} ->
            Logger.error("Failed to delete key '#{key}' from index: #{inspect(reason)}")
            send_resp(conn, 500, "Internal Server Error")
        end

      :not_found ->
        send_resp(conn, 404, "Not Found")

      {:error, reason} ->
        Logger.error("Error getting key '#{key}' for deletion: #{inspect(reason)}")
        send_resp(conn, 500, "Internal Server Error")
    end
  end

  match "/:key", via: :unlink do
    key = conn.params["key"]

    case Mkv.Index.get(key) do
      {:ok, value_location_info} ->
        case Mkv.Index.mark_unlinked(key, value_location_info) do
          :ok ->
            send_resp(conn, 204, "")
          {:error, reason} ->
            Logger.error("Failed to unlink key '#{key}': #{inspect(reason)}")
            send_resp(conn, 500, "Internal Server Error")
        end

      :not_found ->
        send_resp(conn, 404, "Not Found")

      {:error, reason} ->
        Logger.error("Error getting key '#{key}' for unlinking: #{inspect(reason)}")
        send_resp(conn, 500, "Internal Server Error")
    end
  end

  defp put_to_volume(volume_host_port, path, body) do
    url = "http://" <> volume_host_port <> path
    Logger.debug("Putting key to volume: #{url}")
    Finch.build(:put, url, [], body)
    |> Finch.request(Mkv.Finch)
  end

  defp delete_from_volume(volume_host_port, path) do
    url = "http://" <> volume_host_port <> path
    Logger.debug("Deleting key from volume: #{url}")
    Finch.build(:delete, url)
    |> Finch.request(Mkv.Finch)
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
