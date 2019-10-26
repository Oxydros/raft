defmodule Raft.LocalStorage do
  @moduledoc """
  Wrapper around rockdb to handle persistent storage
  """

  use GenServer

  @db_dir "#{Application.app_dir(:raft)}/.raft"
  @db_path "#{@db_dir}/storage.db"

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, [name: __MODULE__])

  @doc """
  Retrieve the rocksdb reference on the local storage
  """
  def get_db, do: GenServer.call({:local, __MODULE__}, :get_db)
  def save_data(key, value), do: GenServer.call({:local, __MODULE__}, {:save, key, value})
  def get_data(key), do: GenServer.call({:local, __MODULE__}, {:get, key})

  def init(_args) do
    # if !File.exists?(@db_dir), do: File.mkdir_p!(@db_dir)
    # {:ok, db_ref} = :rocksdb.open(to_charlist(@db_path), [create_if_missing: true])
    {:ok, nil}
  end

  def handle_call(:get_db, _from, db_ref) do
    {:reply, db_ref, db_ref}
  end

  def handle_call({:save, key, value}, _from, db_ref) do
    # :rocksdb.put(db_ref, key, value, [])
    {:reply, :ok, db_ref}
  end

  def handle_call({:get, key}, _from, db_ref) do
    {:reply, nil, db_ref}
  end
end
