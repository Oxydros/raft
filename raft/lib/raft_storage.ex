defmodule Raft.LocalStorage do

  use GenServer

  @db_path "#{Application.app_dir(:raft)}/.raft/storage.db"

  def start_link(args), do: GenServer.start_link(__MODULE__, args, [name: {:local, __MODULE__}])

  @doc """
  Retrieve the rocksdb reference on the local storage
  """
  def get_db, do: GenServer.call({:local, __MODULE__}, :get_db)
  def save_data(key, value), do: GenServer.call({:local, __MODULE__}, {:save, key, value})
  def get_data(key), do: GenServer.call({:local, __MODULE__}, {:get, key})

  def init(_args) do
    File.mkdir_p("")
    {:ok, db_ref} = :rocksdb.open(@db_path, [create_if_missing: true])
    {:ok, db_ref}
  end

  def handle_call(:get_db, _from, db_ref) do
    {:reply, db_ref, db_ref}
  end

  def handle_call({:save, key, value}, _from, db_ref) do
    :rocksdb.put(db_ref, key, value, [])
    {:reply, :ok, db_ref}
  end

  def handle_call({:get, key}, _from, db_ref) do
    {:reply, :rocksdb.get(db_ref, key, []), db_ref}
  end
end
