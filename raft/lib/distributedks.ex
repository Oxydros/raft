defmodule DistributedKS do
  @moduledoc """
  Simple Distributed Key-Value Store
  """
  use DistributedGenserver

  def cluster, do: [:"toto@Louiss-MacBook-Pro", :"titi@Louiss-MacBook-Pro", :"tata@Louiss-MacBook-Pro"] |> Enum.reject(fn n -> n == Node.self() end)

  #########################################
  #          Client Implementation        #
  #########################################

  def get(key), do: DistributedGenserver.call_read(__MODULE__, {:get, key})
  def put(key, value), do: DistributedGenserver.call_write(__MODULE__, {:put, key, value})
  def state(), do: DistributedGenserver.call_read(__MODULE__, :get_full_state)


  #########################################
  #          Server Implementation        #
  #########################################

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      restart: :transient,
      type: :worker
    }
  end

  def start_link() do
    DistributedGenserver.start_link(__MODULE__, nil, cluster(), [name: __MODULE__, debug: [:trace]])
  end

  def initialize(_) do
    {:ok, %{}}
  end

  def handle_call_read({:get, key}, _from, state) do
    {:reply, Map.get(state, key)}
  end

  def handle_call_read(:get_full_state, _from, state) do
    {:reply, state}
  end

  def handle_write({:put, key, value}, state) do
    Map.put(state, key, value)
  end
end
