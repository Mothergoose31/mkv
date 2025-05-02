defmodule Mkv.Operations do
  require Logger

  @doc """
  Rebalances data across volumes.
  Used when volumes are added or removed.
  """
  def rebalance(config) do
    db_path = config[:db_path]
    volumes = config[:volumes]
    replicas = config[:replicas]

    Logger.info("Starting rebalance operation")
    Logger.info("DB Path: #{db_path}")
    Logger.info("Volumes: #{inspect(volumes)}")
    Logger.info("Replicas: #{replicas}")

    # Open the RocksDB instance
    db_path_charlist = String.to_charlist(db_path)
    {:ok, db} = :rocksdb.open(db_path_charlist, [create_if_missing: false])

    try do
      # Iterate through all keys
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
      source_volume = List.first(old_volumes)
      data = fetch_data_from_volume(source_volume, path_on_volume)

      Enum.each(volumes_to_add, fn volume ->
        put_to_volume(volume, path_on_volume, data)
      end)
    end

    new_value_location_info = {target_volumes, path_on_volume}
    new_value = :erlang.term_to_binary(new_value_location_info)
    :rocksdb.put(db, key, new_value, [])

    Enum.each(volumes_to_remove, fn volume ->
      delete_from_volume(volume, path_on_volume)
    end)
  end

  defp scan_volume_files(volume) do
    Logger.info("Scanning volume #{volume}")
    # TODO: Implement
    []
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
    # TODO: Implement
    ""
  end

  defp put_to_volume(volume, path, _data) do
    url = "http://" <> volume <> path
    Logger.debug("Putting data to volume: #{url}")
    # TODO: Implement
    :ok
  end

  defp delete_from_volume(volume, path) do
    url = "http://" <> volume <> path
    Logger.debug("Deleting data from volume: #{url}")
    # TODO: Implement
    :ok
  end
end
