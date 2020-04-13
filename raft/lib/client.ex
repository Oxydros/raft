defmodule Raft.Client do
  @moduledoc """
  Local Client

  Register to the FSM using Raft.Protocol.registerClient/0 and keep the id in state
  """
  use GenServer

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
    :gen_statem.start_link({:local, __MODULE__}, __MODULE__, args, [])
  end

  def init(%{cluster: cluster, module: module, raft_cluster_id: id}) do
    {:ok, protocol_pid} = Raft.Protocol.start_link(%{cluster: cluster, module: module, raft_cluster_id: id})
    :timer.sleep(:timer.seconds(15))
    {:ok, client_id} = Raft.Protocol.registerClient()
    {:ok, %{
      protocol_pid: protocol_pid,
      client_id: client_id
    }}
  end

  def terminate(_reason, _state, _data) do
    :ok
  end

  def handle_call({:write_log, new_log, timeout}, _from, state) do
    seq_num = Enum.random(1..100_000)
    {:reply, Raft.Protocol.write_log(state.client_id, seq_num, new_log, timeout), state}
  end
end
