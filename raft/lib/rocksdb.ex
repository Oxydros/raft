defmodule RocksDB do
  @moduledoc """
  Wrapper around rockdb to handle persistent storage
  """

  use GenServer

  def start_link(raft_id), do: GenServer.start_link(__MODULE__, raft_id, [debug: [:trace]])

  @doc """
  Retrieve the rocksdb reference on the local storage
  """
  def ref(pid), do: GenServer.call(pid, :get_ref)
  def put(pid, key, value, options \\ []), do: GenServer.call(pid, {:put, key, value, options})
  def get(pid, key), do: GenServer.call(pid, {:get, key})
  def get!(pid, key) do
    {:ok, v} = GenServer.call(pid, {:get, key})
    v
  end

  def init(path) do
    {:ok, db_ref} = :rocksdb.open(path, [create_if_missing: true])

    {:ok, %{db_path: path, db_ref: db_ref}}
  end

  # Clean terminate
  def terminate(_reason, state) do
    :rocksdb.close(state.db_ref)
  end

  def handle_call(:get_ref, _from, state) do
    {:reply, state.db_ref, state}
  end

  def handle_call({:put, key, value, options}, from, state) do
    if Keyword.get(options, :sync, false) == false do
      GenServer.reply(from, :ok)
    end

    :rocksdb.put(state.db_ref, key, value |> :erlang.term_to_binary, options)

    if Keyword.get(options, :sync, false) == false do
      {:noreply, :ok, state}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:get, key}, _from, state) do
    reply = case :rocksdb.get(state.db_ref, key, []) do
      {:ok, value} -> {:ok, value |> :erlang.binary_to_term}
      :not_found -> :not_found
      err -> err
    end
    {:reply, reply, state}
  end
end
