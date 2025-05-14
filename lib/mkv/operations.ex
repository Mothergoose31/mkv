defmodule Mkv.Operations do
  require LoggerS

  @doc """
  Rebalances data across volumes.
  Used when volumes are added or removed..
  """
  def rebalance(config) do
    db_path = config[:db_path]
    volumes = config[:volumes]
    replicas = config[:replicas]

    Logger.info("Starting rebalance operation")
    Logger.info("DB Path: #{db_path}")
    Logger.info("Volumes: #{inspect(volumes)}")
    Logger.info("Replicas: #{replicas}")

    db_path_charlist = String.to_charlist(db_path)
    {:ok, db} = :rocksdb.open(db_path_charlist, [create_if_missing: false])

    try do
      {:ok, iterator} = :rocksdb.iterator(db, [])
      :rocksdb.iterator_move(iterator, :first)

      process_keys_for_rebalance(iterator, db, volumes, replicas)

      :rocksdb.iterator_close(iterator)
      Logger.info("Rebalance operation completed successfully")
      :ok
    rescue
      e ->
        Logger.error("Error during rebalance: #{inspect(e)}")
        {:error, e}
    after
      :rocksdb.close(db)
    end
  end

  @doc """
  Rebuilds the index from volumes.
  Used when the index is lost or corrupted.
  """
  def rebuild(config) do
    source_volumes = config[:volumes]
    new_db_path = config[:db_path]
    replicas = config[:replicas]

    Logger.info("Starting rebuild operation")
    Logger.info("New DB Path: #{new_db_path}")
    Logger.info("Source Volumes: #{inspect(source_volumes)}")

    db_path_charlist = String.to_charlist(new_db_path)
    {:ok, db} = :rocksdb.open(db_path_charlist, [create_if_missing: true])

    try do
      results =
        source_volumes
        |> Enum.map(&scan_volume_files(&1))
        |> List.flatten()
        |> group_by_key()
        |> Enum.map(&store_in_index(&1, db, source_volumes, replicas))

      successful = Enum.count(results, fn r -> r == :ok end)
      failed = Enum.count(results, fn r -> r != :ok end)

      Logger.info("Rebuild completed: #{successful} keys indexed, #{failed} failures")
      {:ok, %{successful: successful, failed: failed}}
    rescue
      e ->
        Logger.error("Error during rebuild: #{inspect(e)}")
        {:error, e}
    after
      :rocksdb.close(db)
    end
  end

  defp process_keys_for_rebalance(iterator, db, volumes, replicas) do
    case :rocksdb.iterator_move(iterator, :next) do
      {:ok, key, value} when is_binary(key) and is_binary(value) ->
        if not String.starts_with?(key, "unlinked:") do
          rebalance_key(key, value, db, volumes, replicas)
        end
        process_keys_for_rebalance(iterator, db, volumes, replicas)
      _ ->
        :ok
    end
  end

  defp rebalance_key(key, value, db, volumes, replicas) do
    value_location_info = :erlang.binary_to_term(value)
    {old_volumes, path_on_volume} = value_location_info

    target_volumes = Enum.take(volumes, replicas)

    volumes_to_add = target_volumes -- old_volumes
    volumes_to_remove = old_volumes -- target_volumes

    if length(volumes_to_add) > 0 do
      source_volume =
        Enum.find(old_volumes, &(&1 in volumes)) || List.first(old_volumes)

      if source_volume do
        case fetch_data_from_volume(source_volume, path_on_volume) do
          {:ok, data_to_copy} ->
            Enum.each(volumes_to_add, fn volume_to_add ->
              case put_to_volume(volume_to_add, path_on_volume, data_to_copy) do
                :ok ->
                  Logger.debug("Successfully put data for key '#{key}' to volume '#{volume_to_add}' at path '#{path_on_volume}'")
                {:error, put_error} ->
                  Logger.error("Failed to put data for key '#{key}' to volume '#{volume_to_add}' at path '#{path_on_volume}'. Error: #{inspect(put_error)}")
              end
            end)
          {:error, fetch_error} ->
            Logger.error("Failed to fetch data for key '#{key}' from source volume '#{source_volume}' at path '#{path_on_volume}'. Cannot replicate to new volumes. Error: #{inspect(fetch_error)}")
        end
      else
        Logger.error("No suitable source volume found in current configuration for key '#{key}' from old volumes: #{inspect(old_volumes)}. Cannot replicate to new volumes.")
      end
    end

    new_value_location_info = {target_volumes, path_on_volume}
    new_value = :erlang.term_to_binary(new_value_location_info)
    case :rocksdb.put(db, key, new_value, []) do
      :ok ->
        Logger.debug("Successfully updated index for key '#{key}' to new volumes: #{inspect(target_volumes)}")
      {:error, db_put_error} ->
        Logger.error("Failed to update index for key '#{key}'. Error: #{inspect(db_put_error)}")
    end

    Enum.each(volumes_to_remove, fn volume_to_remove ->
      case delete_from_volume(volume_to_remove, path_on_volume) do
        :ok ->
          Logger.debug("Successfully deleted data for key '#{key}' from volume '#{volume_to_remove}' at path '#{path_on_volume}'")
        {:error, delete_error} ->
          Logger.error("Failed to delete data for key '#{key}' from volume '#{volume_to_remove}' at path '#{path_on_volume}'. Error: #{inspect(delete_error)}")
      end
    end)

    :ok
  end

  defp scan_volume_files(volume) do
    Logger.info("Scanning volume #{volume}")

    url = "http://#{volume}/_list"

    case Finch.build(:get, url) |> Finch.request(Mkv.Finch) do
      {:ok, %{status: 200, body: body}} ->
        case Jason.decode(body) do
          {:ok, file_list} ->
            Enum.map(file_list, fn %{"path" => path} ->
              key = String.replace_prefix(path, "/data/", "")
              {key, volume, path}
            end)

          {:error, _} ->
            Logger.error("Failed to parse JSON response from volume #{volume}")
            []
        end

      _ ->
        Logger.error("Failed to get file listing from volume #{volume}")
        []
    end
  end

  defp group_by_key(files) do
    []
    Enum.group_by(files, fn {key, _volume, _path} -> key end,
                 fn {_key, volume, path} -> {volume, path} end)
    |> Enum.map(fn {key, locations} -> {key, locations} end)
  end

  defp store_in_index({key, locations}, db, all_volumes, replicas) do
    # TODO: Implement
    []
    volumes = Enum.map(locations, fn {volume, _path} -> volume end)
    path = case locations do
      [{_volume, path} | _] -> path
      _ -> nil
    end

    if path && length(volumes) > 0 do

      target_volumes = if length(volumes) >= replicas do
        Enum.take(volumes, replicas)
      else
        additional_volumes = all_volumes -- volumes
        volumes ++ Enum.take(additional_volumes, replicas - length(volumes))
      end

      value_location_info = {target_volumes, path}
      serialized = :erlang.term_to_binary(value_location_info)
      :rocksdb.put(db, key, serialized, [])
    else
      {:error, :invalid_location_data}
    end
  end

  defp fetch_data_from_volume(volume, path) do
    url = "http://" <> volume <> path
    Logger.debug("Fetching data from volume: #{url}")

    case Finch.build(:get, url) |> Finch.request(Mkv.Finch) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}
      {:ok, %{status: status, body: body}} ->
        Logger.error("Failed to fetch data from #{url}. Status: #{status}, Body: #{inspect(body)}")
        {:error, {:http_error, status}}
      {:error, reason} ->
        Logger.error("Error fetching data from #{url}. Reason: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp put_to_volume(volume, path, data) do
    url = "http://" <> volume <> path
    Logger.debug("Putting data to volume: #{url}")
    headers = [{"content-type", "application/octet-stream"}]

    case Finch.build(:put, url, headers, data) |> Finch.request(Mkv.Finch) do
      {:ok, %{status: status}} when status in [200, 201, 204] ->
        :ok
      {:ok, %{status: status, body: body}} ->
        Logger.error("Failed to put data to #{url}. Status: #{status}, Body: #{inspect(body)}")
        {:error, {:http_error, status}}
      {:error, reason} ->
        Logger.error("Error putting data to #{url}. Reason: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp delete_from_volume(volume, path) do
    url = "http://" <> volume <> path
    Logger.debug("Deleting data from volume: #{url}")

    case Finch.build(:delete, url) |> Finch.request(Mkv.Finch) do
      {:ok, %{status: status}} when status in [200, 204] ->
        :ok
      {:ok, %{status: status, body: body}} ->
        Logger.error("Failed to delete data from #{url}. Status: #{status}, Body: #{inspect(body)}")
        {:error, {:http_error, status}}
      {:error, reason} ->
        Logger.error("Error deleting data from #{url}. Reason: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
