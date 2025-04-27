defmodule Mkv.Router do
  use Plug.Router
  import Plug.Conn
  require Logger

  plug :match
  plug :fetch_query_params
  plug :dispatch

  # GET /key
  get "/:key" do
    key = conn.params["key"]

    case Mkv.Index.get(key) do
      {:ok, value_location_info} ->
        # TODO: Construct proper Nginx redirect based on value_location_info
        #       Example: value_location_info = {"volume_host:port", "/path/on/volume"}
        #       Redirect URL = "http://volume_host:port/path/on/volume"
        # For now, just return the info found
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{location: value_location_info})) # Requires Jason dep

      :not_found ->
        send_resp(conn, 404, "Not Found")

      {:error, reason} ->
        Logger.error("Error getting key '#{key}': #{inspect(reason)}")
        send_resp(conn, 500, "Internal Server Error")
    end
  end

  # PUT /key
  put "/:key" do
    key = conn.params["key"]

    # 1. Read the request body
    {:ok, body, conn} = read_body(conn)

    # 2. Determine Target Volume(s) (Placeholder)
    # TODO: Implement robust volume selection based on key, replicas, available volumes
    # TODO: Get volume list from config or a dedicated VolumeManager
    # Placeholder: Assume we write to the first replica count volumes
    all_volumes = Mkv.Application.load_config()[:volumes] # Temporary access
    replicas = Mkv.Application.load_config()[:replicas] # Temporary access
    target_volumes = Enum.take(all_volumes, replicas)

    # 3. Store on Volume(s) via HTTP PUT
    # TODO: Determine actual path/filename on volume (e.g., based on key hash?)
    path_on_volume = "/data/" <> key # Very basic placeholder path

    # Use Task.async_stream for concurrent puts
    results =
      target_volumes
      |> Task.async_stream(&put_to_volume(&1, path_on_volume, body), ordered: false, timeout: 10000) # 10 sec timeout
      |> Enum.to_list()

    successful_puts =
      Enum.filter(results, fn
        {:ok, {:ok, %{status: status}}} when status >= 200 and status < 300 -> true
        _ -> false
      end)

    # 4. Update Index if enough replicas succeeded
    if length(successful_puts) >= replicas do
      # TODO: Define value_location_info properly (e.g., {volume_list, path_on_volume})
      value_location_info = {target_volumes, path_on_volume}
      case Mkv.Index.put(key, value_location_info) do
        :ok ->
          send_resp(conn, 201, "Created")
        {:error, reason} ->
          Logger.error("Failed to update index for key '#{key}': #{inspect(reason)}")
          # TODO: How to handle PUT success but index fail? Rollback delete?
          send_resp(conn, 500, "Internal Server Error (Index Update Failed)")
      end
    else
      Logger.error("Failed to write enough replicas for key '#{key}'. Success: #{length(successful_puts)}, Needed: #{replicas}")
      # TODO: Rollback successful puts by sending DELETE requests?
      send_resp(conn, 503, "Service Unavailable (Failed to write replicas)")
    end
  end

  # Helper function to PUT data to a single volume
  # Assumes volume format is "host:port"
  defp put_to_volume(volume_host_port, path, body) do
    url = "http://" <> volume_host_port <> path
    Logger.debug("Putting key to volume: #{url}")
    # TODO: Handle potential Finch errors (e.g., :nxdomain, :timeout, :connection_refused)
    Finch.build(:put, url, [], body)
    |> Finch.request(Mkv.Finch)
  end

  # DELETE /key
  delete "/:key" do
    # TODO: Implement DELETE logic using Mkv.Index.delete/1
    key = conn.params["key"]
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(204, "DELETE /#{key} (Not Implemented)")
  end

  # UNLINK /key (Virtual Delete)
  match "/:key", via: :unlink do
    # TODO: Implement UNLINK logic (Maybe add Mkv.Index.unlink/1?)
    key = conn.params["key"]
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(204, "UNLINK /#{key} (Not Implemented)")
  end

  # GET /prefix?list
  get "/:prefix" do
    # Needs Plug.Conn.fetch_query_params before accessing conn.params
    # conn = fetch_query_params(conn)
    case conn.params["list"] do
      _list_param_present ->
        # TODO: Implement LIST logic using Mkv.Index.list_prefix/1
        prefix = conn.params["prefix"]
        conn
        |> put_resp_content_type("application/json") # Or text/plain?
        |> send_resp(200, "{\"keys\": [\"LIST /#{prefix} (Not Implemented)\"]}")

      _ ->
        # Fallback to regular GET if '?list' is not present
        key = conn.params["prefix"]
        # TODO: Implement GET logic using Mkv.Index.get/1
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(200, "GET /#{key} (Not Implemented - fallback)")
    end
  end

  # GET /?unlinked
  get "/" do
    # Needs Plug.Conn.fetch_query_params before accessing conn.params
    # conn = fetch_query_params(conn)
    case conn.params["unlinked"] do
      _unlinked_param_present ->
        # TODO: Implement UNLINKED listing logic (requires index changes)
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, "{\"keys\": [\"LIST /?unlinked (Not Implemented)\"]}")
      _ ->
        # Handle GET / (root) if needed, otherwise 404
        send_resp(conn, 404, "Not Found")
    end
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
