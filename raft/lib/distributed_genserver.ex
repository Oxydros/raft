defmodule DistributedGenserver do
  @moduledoc """
  Documentation for DistributedGenserver.
  """

  require Logger

  defmacro __using__(_) do
    quote location: :keep do
      # @behaviour DistributedGenServer

      use GenServer
      require Logger

      def init(%{cluster: cluster, timeout: timeout, init_args: init_args}) do
        Logger.info("[DistributedGenserver] #{__MODULE__} -- #{inspect init_args} on cluster -- #{inspect cluster}")
        {:ok, state} = initialize(init_args)
        {:ok, %{state: state, timeout: timeout}, {:continue, {:setup_raft, cluster}}}
      end

      def handle_cast_read(_req, _state) do
        :ok
      end

      def handle_call_read(_req, _from, _state) do
        :noreply
      end

      def handle_write(req, state) do
        Logger.error("No handle_call_write for #{inspect req}")
        {:noreply, state}
      end

      def handle_continue({:setup_raft, cluster}, state) do
        {:ok, raft_pid} = Raft.Protocol.start_link(%{cluster: cluster, module: __MODULE__, pid: self(), id: "distributedKS"})
        {:noreply, state |> Map.put(:raft_pid, raft_pid)}
      end

      def handle_call({:read, request}, from, state) do
        case handle_call_read(request, from, state.state) do
          :noreply ->
            {:reply, :ok, state}
          {:reply, reply} ->
            {:reply, reply, state}
        end
      end

      def handle_call({:write, request}, from, state) do
        log = request |> :erlang.term_to_binary
        spawn fn ->
          reply = Raft.Protocol.write_log(log, state.timeout)
          GenServer.reply(from, reply)
        end
        {:noreply, state}
      end

      def handle_call({:apply, request}, _from, state) do
        case handle_write(request, state.state) do
          {:noreply, new_state} ->
            {:reply, :ok, %{state | state: new_state}}
          {:reply, reply, new_state} ->
            {:reply, reply, %{state | state: new_state}}
        end
      end

      def handle_cast({:read, request}, state) do
        _ = handle_cast_read(request, state.state)
        {:noreply, state}
      end

      def handle_info(_, state) do
        {:noreply, state}
      end

      def terminate(_reason, _state) do
        :ok
      end

      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      # Called by the Raft Protocol when the log entry is commited
      def apply_log_entry(log_entry, fsm_data) do
        request = :erlang.binary_to_term(log_entry)
        GenServer.call(fsm_data.pid, {:apply, request})
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
    GenServer.start_link(module, %{cluster: cluster, init_args: init_args, timeout: options |> Keyword.get(:timeout, 4_500)}, options)
  end
end
