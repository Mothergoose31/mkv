defmodule Mkv.Index do
  use GenServer

  require Logger


  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  def put(key, value_location_info) do

    serialized_value = :erlang.term_to_binary(value_location_info)
    GenServer.call(__MODULE__, {:put, key, serialized_value})
  end

  def get(key) do
    case GenServer.call(__MODULE__, {:get, key}) do
      {:ok, serialized_value} when is_binary(serialized_value) ->
        {:ok, :erlang.binary_to_term(serialized_value)}
      :not_found ->
        :not_found
      {:error, reason} ->
        {:error, reason}
      other ->
        Logger.error("Unexpected value from RocksDB get for key '#{key}': #{inspect(other)}")
        {:error, :unexpected_db_value}
    end
  end

  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  def list_prefix(prefix) do
    GenServer.call(__MODULE__, {:list_prefix, prefix})
  end


  @impl true
  def init(config) do
    db_path = config[:db_path] |> String.to_charlist()
    opts = [create_if_missing: true]

    case :rocksdb.open(db_path, opts) do
      {:ok, db_ref} ->
        Logger.info("RocksDB opened at #{inspect(db_path)}")
        {:ok, %{db: db_ref, path: db_path}}
      {:error, reason} ->
        Logger.error("Failed to open RocksDB at #{inspect(db_path)}: #{inspect(reason)}")
        {:stop, {:rocksdb_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do

    reply = :rocksdb.put(state.db, key, value)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do

    reply = :rocksdb.get(state.db, key)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    reply = :rocksdb.delete(state.db, key)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:list_prefix, prefix}, _from, state) do
    Logger.warning("list_prefix not yet implemented for prefix: #{prefix}")
    {:reply, {:ok, []}, state} # Return empty list for now
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Closing RocksDB (#{inspect(state.path)}): Reason: #{inspect(reason)}")
    if Map.has_key?(state, :db) and state.db != nil do
       :rocksdb.close(state.db)
    end
    :ok
  end
end
