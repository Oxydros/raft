defmodule Raft.Client do
  @moduledoc """
  Local Client

  Register to the FSM using Raft.Protocol.registerClient/0 and keep the id in state
  """
  use GenServer

  require Logger

  @doc """
  Write a log on the distributed FSM, and apply it.
  """
  def write_log(pid, new_log, timeout \\ 10_000)
  def write_log(pid, new_log, timeout) when not is_list(new_log) do
    GenServer.call(pid, {:write_log, new_log, timeout}, timeout + 500)
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :transient,
      type: :worker
    }
  end

  def start_link(%{cluster: _c, module: _m, raft_cluster_id: _i} = args) do
    GenServer.start_link(__MODULE__, args)
  end

  def init(%{cluster: _cluster, module: _module, raft_cluster_id: _id}=args) do
    # Launch the protocol
    {:ok, protocol_pid} = Raft.Protocol.start_link(args)
    {:ok, %{
      protocol_pid: protocol_pid,
      client_id: nil
    }}
  end

  def terminate(_reason, _state, _data) do
    :ok
  end

  def try_register(retry, wait_ms \\ 0)
  def try_register(0, _), do: :no_leader
  def try_register(retry, wait_ms) do
    Logger.debug("TRYING REGISTER")
    case Raft.Protocol.registerClient() do
      :no_leader ->
        :timer.sleep(wait_ms)
        try_register(retry - 1, wait_ms)
      {:ok, client_id} ->
        client_id
    end
  end

  def handle_call({:write_log, new_log, timeout}, _from, state) do
    # If we dont have a client ID, try to get one
    client_id = if is_nil(state.client_id) do
      case try_register(5, 1_000) do
        :no_leader ->
          :error
        client_id ->
          client_id
      end
    else state.client_id end

    case client_id do
      :error ->
        {:reply, :error, state}
      client_id ->
        seq_num = Enum.random(1..100_000)
        {:reply, Raft.Protocol.write_log(client_id, seq_num, new_log, timeout), %{state | client_id: client_id}}
    end
  end
end
