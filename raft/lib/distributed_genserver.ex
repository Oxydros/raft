defmodule DistributedGenserver do
  @moduledoc """
  Documentation for DistributedGenserver.
  """

  use GenServer
  require Logger

  defmacro __using__(_) do
    quote location: :keep do
      # @behaviour DistributedGenServer
      require Logger

      def handle_cast_read(_req, _state) do
        :ok
      end

      def handle_call_read(_req, _from, _state) do
        :noreply
      end

      def handle_write(req, state) do
        raise "No handle_call_write for #{inspect req}"
      end

      def handle_continue({:setup_raft, cluster}, state) do
        {:ok, raft_pid} = Raft.Client.start_link(%{cluster: cluster, module: __MODULE__, raft_cluster_id: "distributedKS"})
        {:noreply, state |> Map.put(:raft_pid, raft_pid)}
      end

      def handle_call({:read, request}, from, state) do
        case state.mod.handle_call_read(request, from, state.state) do
          :noreply ->
            {:reply, :ok, state}
          {:reply, reply} ->
            {:reply, reply, state}
        end
      end

      def handle_call({:write, request}, from, state) do
        spawn_link fn ->
          log = request |> :erlang.term_to_binary
          reply =  Raft.Client.write_log(state.raft_pid, log, state.timeout)
          GenServer.reply(from, reply)
        end
        {:noreply, state}
      end

      def handle_call({:apply, request}, _from, state) do
        case state.mod.handle_write(request, state.state) do
          {:noreply, new_state} ->
            {:reply, :ok, %{state | state: new_state}}
          {:reply, reply, new_state} ->
            {:reply, reply, %{state | state: new_state}}
        end
      end

      def handle_cast({:read, request}, state) do
        _ = state.mod.handle_cast_read(request, state.state)
        {:noreply, state}
      end

      # Called by the Raft Protocol when the log entry is commited
      def apply_log_entry(log_entry, fsm_data) do
        request = :erlang.binary_to_term(log_entry)
        GenServer.call(fsm_data.pid, {:apply, request})
      end

      def init(%{mod: mod, cluster: cluster, timeout: timeout, init_args: init_args}) do
        Logger.info("[DistributedGenserver] #{__MODULE__} -- #{inspect init_args} on cluster -- #{inspect cluster}")
        case mod.init(init_args) do
          {:ok, state} ->
            {:ok, %{state: state, timeout: timeout, mod: mod}, {:continue, {:setup_raft, cluster}}}
          other ->
            other
        end
      end

      defoverridable handle_cast_read: 2, handle_call_read: 3, handle_write: 2
    end
  end

  def call_read(module, request, timeout \\ 5_000) do
    GenServer.call(module, {:read, request}, timeout)
  end

  def cast_read(module, request) do
    GenServer.cast(module, {:read, request})
  end

  def call_write(module, request, timeout \\ 5_000) do
    GenServer.call(module, {:write, request}, timeout)
  end

  def start_link(module, init_args, cluster, options \\ []) when is_atom(module) and is_list(cluster) and is_list(options) do
    GenServer.start_link(module, %{mod: module, cluster: cluster, init_args: init_args, timeout: options |> Keyword.get(:timeout, 4_500)}, options)
  end
end
