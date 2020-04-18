defmodule Raft.Protocol do
  @moduledoc """
  :gen_statem FSM implementing the Raft procotol
  """

  require Logger

  @db_dir "#{Application.app_dir(:raft)}/.raft"
  @state_key "RAFT_STATE"

  #########################################
  #               FSM Client              #
  #########################################

  @doc """
  Open a new client session to be used for all futher requests
  """
  def registerClient(timeout \\ 10_000) do
    send_raft_request(:register_client, timeout)
  end

  @doc """
  Write a log on the distributed FSM, and apply it.
  """
  def write_log(client_id, sequence_num, new_log, timeout \\ 10_000)
  def write_log(client_id, sequence_num, new_log, timeout) when not is_list(new_log) do
    send_raft_request({client_id, sequence_num, {:write_log, new_log}}, timeout)
  end

  #########################################
  #            FSM Server Utils           #
  #########################################

  def send_raft_request(req, timeout \\ 10_000) do
    case get_leader!() do
      nil -> :no_leader
      leader_node ->
        :gen_statem.call({__MODULE__, leader_node}, {:raft, req}, timeout)
    end
  end

  def append_entries_rpc(asking_node, req) do
    {:ok, {_term, _success}=res} = :gen_statem.call(__MODULE__, {:append_entries, req})
    :gen_statem.cast({__MODULE__, asking_node}, {:resp_append, res, Node.self})
    :ok
  end

  def request_vote_rpc(asking_node, req) do
    {:ok, {_term, _vote}=res} = :gen_statem.call(__MODULE__, {:request_vote, req})
    :gen_statem.cast({__MODULE__, asking_node}, {:resp_vote, res, Node.self})
    :ok
  end

  def get_leader!() do
    {:ok, leader} = :gen_statem.call(__MODULE__, :get_leader)
    leader
  end

  #########################################
  #        Protocol implementation        #
  #########################################

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
  def stop(), do: :gen_statem.stop(__MODULE__)

  def callback_mode() do
    [:state_functions, :state_enter]
  end

  def init(%{cluster: _cluster, module: module, raft_cluster_id: raft_id} = args) do
    Logger.debug("[Raft - Protocol] Starting FSM for #{inspect module}-#{inspect raft_id} --- #{Node.self()} --- #{inspect args}")

    File.mkdir_p(@db_dir)
    path = '#{@db_dir}/#{raft_id}.db'
    {:ok, storage_pid} = RocksDB.start_link(path)

    persistent_storage = case RocksDB.get(storage_pid, @state_key) do
      :not_found -> %{
        currentTerm: 0,
        votedFor: nil,
        logs: [],
      }
      {:ok, v} -> v
      err ->
        Logger.error("[Raft - Protocol] Error while reading local state #{inspect err}")
        throw err
    end

    Logger.debug("[Raft - Protocol] Read #{inspect persistent_storage} from local storage")

    initial_state = Map.merge(args, %{
      # Volatile -- all
      # Keeps track of latest commited index
      commitIndex: 0,
      # Keeps track of latest applied index
      lastApplied: 0,
      # Keeps track of current leader
      leader: nil,
      # Keeps track of connected clients
      clients: %{},

      # Volatile -- candidates
      votes: 0,

      # Volatile -- leaders
      nextIndex: [],
      matchIndex: [],
      replyMap: %{},

      # Elixir
      storage: storage_pid
    })

    initial_state = Map.merge(initial_state, persistent_storage)
    {:ok, :follower, initial_state, []}
  end

  def terminate(_reason, _state, fsm_data) do
    :ok = save_to_persistent_storage(fsm_data)
  end

  #########################################
  #            Follower logic             #
  #########################################

  def follower(:enter, _oldState, fsm_data) do
    Logger.debug("[follower] Entering follower state #{inspect(fsm_data, pretty: true)}")
    {:keep_state, fsm_data, [get_election_timer()]}
  end

  def follower({:timeout, :election}, :election_timeout, fsm_data) do
    Logger.debug("[follower] Election timed-out ! Becoming a candidate")
    {:next_state, :candidate, fsm_data}
  end

  def follower({:call, from}, {:append_entries, req}, fsm_data) do
    Logger.debug("[follower] Received new entries with req #{inspect(req, pretty: true)}. Reset timeout")

    %{
      term: l_term,
      leaderID: l_id,
      prevLogIndex: l_prev_idx,
      prevLogTerm: l_prev_term,
      entries: entries,
      leaderCommit: l_commit_idx
    } = req

    %{currentTerm: savedTerm, logs: savedLogs} = fsm_data

    log_term_at_idx = fsm_data[:logs] |> Enum.at(l_prev_idx - 1)
    log_term_at_idx = log_term_at_idx[:term] || -1

    # We dont agree upon the version of logs, or follower have a better term
    failure? = l_term < fsm_data[:currentTerm] || log_term_at_idx != l_prev_term

    fsm_data = if not failure? do
      # Take all logs we agree upon, and add the new entries
      # This will overwrite eventual conflicts
      new_logs = (fsm_data[:logs] |> Enum.take(l_prev_idx)) ++ entries
      new_commit_idx = min(l_commit_idx, new_logs |> Enum.count)

      fsm_data = fsm_data
        |> Map.put(:commitIndex, new_commit_idx)

      #Apply logs from fsm_data[:lastApplied] to new_commit_idx
      fsm_data = apply_logs(fsm_data, fsm_data.lastApplied, fsm_data.commitIndex, new_logs)
      fsm_data
        |> Map.put(:logs, new_logs)
        |> Map.put(:currentTerm, l_term)
        |> Map.put(:leader, l_id)
    else
      fsm_data
    end

    # Setup current term
    fsm_data = if l_term < fsm_data[:currentTerm], do: change_term(fsm_data, l_term), else: fsm_data

    # Only save if change in term or got new entries
    cond do
      fsm_data.currentTerm != savedTerm ->
        :ok = save_to_persistent_storage(fsm_data)
      (entries |> Enum.count) > 0 ->
        :ok = save_to_persistent_storage(fsm_data)
      (savedLogs |> Enum.count) != (fsm_data.logs |> Enum.count) ->
        :ok = save_to_persistent_storage(fsm_data)
      true ->
        :ok
    end

    reply_data = {:ok, {fsm_data[:currentTerm], not failure?}}
    Logger.warn("[follower] Replying #{inspect(reply_data, pretty: true)}")
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  def follower({:call, from}, {:request_vote, req}, fsm_data) do
    Logger.debug("[follower] Received request vote #{inspect(req, [limit: :infinity, pretty: true])}")

    %{
      term: req_term,
      candidateId: candidate_id,
      lastLogIndex: last_c_log_idx,
      lastLogTerm: last_c_log_term
    } = req
    %{currentTerm: current_term, votedFor: vote, logs: logs} = fsm_data

    last_r_log = logs |> Enum.at(-1)
    last_r_log_idx = logs |> Enum.count
    last_r_log_term = last_r_log[:term] || -1

    give_vote? = cond do
      # Candidate does not have the most recent term
      req_term < current_term ->
        false

      # We didn't vote, or already voted for the candidate
      is_nil(vote) || vote == candidate_id ->
        log_up_to_date?(last_c_log_idx, last_c_log_term, last_r_log_idx, last_r_log_term)

      true ->
        false
    end

    # Setup current term
    fsm_data = if req_term < current_term, do: change_term(fsm_data, req_term), else: fsm_data

    # Register the vote
    fsm_data = if give_vote? do
      Logger.debug("[follower] Giving vote to #{inspect(candidate_id)} for term #{req_term}")
      fsm_data
        |> Map.put(:votedFor, candidate_id)
    else
      fsm_data
    end

    :ok = save_to_persistent_storage(fsm_data)

    reply_data = {:ok, {current_term, give_vote?}}
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  # Ignore resp vote
  def follower(:cast, {:resp_vote, {_term, _vote?}, _from}, _fsm_data) do
    :keep_state_and_data
  end

  # Ignore resp append
  def follower(:cast, {:resp_append, {term, success?}, _from}, _fsm_data) do
    Logger.warn("[follower] Got resp append with term #{term} - success #{success?}. Ignoring it !")
    :keep_state_and_data
  end

  def follower(event_type, event_data, fsm_state) do
    handle_common(:follower, event_type, event_data, fsm_state)
  end

  #########################################
  #            Candidate logic            #
  #########################################

  def candidate(:enter, _oldState, fsm_data) do
    Logger.debug("[candidate] Entering candidate state #{inspect(fsm_data, pretty: true)}")

    # Start a new election
    new_term = fsm_data[:currentTerm] + 1

    fsm_data = fsm_data
      |> Map.put(:currentTerm, new_term)
      |> Map.put(:votedFor, Node.self())
      |> Map.put(:votes, 1)
      |> Map.put(:leader, nil)

    last_log = fsm_data[:logs] |> Enum.at(-1)
    last_log_idx = fsm_data[:logs] |> Enum.count
    last_log_term = last_log[:term] || -1

    query = %{
      term: new_term,
      candidateId: Node.self(),
      lastLogIndex: last_log_idx,
      lastLogTerm: last_log_term
    }

    Logger.debug("[candidate] State for this election #{inspect(fsm_data, [pretty: true])}")

    :ok = save_to_persistent_storage(fsm_data)

    :rpc.eval_everywhere(fsm_data.cluster, __MODULE__, :request_vote_rpc, [Node.self, query])
    {:keep_state, fsm_data, [get_election_timer()]}
  end

  def candidate({:timeout, :election}, :election_timeout, _fsm_data) do
    Logger.debug("[candidate] Election timed-out ! Start a new election")
    :repeat_state_and_data
  end

  def candidate({:call, from}, {:append_entries, req}, fsm_data) do
    Logger.debug("[candidate] Received new entries")
    %{
      term: l_term,
      leaderID: l_id,
      prevLogIndex: _l_prev_idx,
      prevLogTerm: _l_prev_term,
      entries: _entries,
      leaderCommit: _l_commit_idx
    } = req
    if l_term > fsm_data[:currentTerm] do
      # Accept entry, go back as follower
      fsm_data = fsm_data
        |> change_term(l_term)
        |> Map.put(:leader, l_id)
      reply_data = {:ok, {fsm_data[:currentTerm], true}}

      :ok = save_to_persistent_storage(fsm_data)
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}, :postpone]}
    else
      # I'm better than the leader, refuse the append entry
      reply_data = {:ok, {fsm_data[:currentTerm], false}}
      {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
    end
  end

  def candidate({:call, from}, {:request_vote, req}, fsm_data) do
    Logger.debug("[candidate] Received request vote #{inspect(req, [limit: :infinity, pretty: true])}")

    %{
      term: req_term,
      candidateId: candidate_id,
      lastLogIndex: last_c_log_idx,
      lastLogTerm: last_c_log_term
    } = req
    %{currentTerm: current_term, logs: logs} = fsm_data

    last_r_log = logs |> Enum.at(-1)
    last_r_log_idx = logs |> Enum.count
    last_r_log_term = last_r_log[:term] || -1

    give_vote? = cond do
      # Already voted
      not is_nil(fsm_data.votedFor) ->
        false
      # Candidate does not have the most recent term
      req_term < current_term ->
        false
      true ->
        log_up_to_date?(last_c_log_idx, last_c_log_term, last_r_log_idx, last_r_log_term)
    end
    # Setup current term
    fsm_data = if req_term < current_term, do: change_term(fsm_data, req_term), else: fsm_data

    reply_data = {:ok, {fsm_data[:currentTerm], give_vote?}}
    if give_vote? do
      fsm_data = fsm_data |> Map.put(:votedFor, candidate_id)
      :ok = save_to_persistent_storage(fsm_data)
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}]}
    else
      :ok = save_to_persistent_storage(fsm_data)
      {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
    end
  end

  def candidate(:cast, {:resp_vote, {term, vote?}, from}, %{currentTerm: current_term, votes: votes}=fsm_data) do
    Logger.debug("[candidate] Received vote #{vote?} term #{term} from #{from}")
    new_votes = if vote?, do: votes + 1, else: votes
    fsm_data = fsm_data |> Map.put(:votes, new_votes)

    cond do
      # Receive response with better term
      # Going back to follower
      term > current_term ->
        fsm_data = change_term(fsm_data, term)
        Logger.debug("[candidate] Invalid local term ! go back to follower")
        :ok = save_to_persistent_storage(fsm_data)
        {:next_state, :follower, fsm_data, [get_election_timer()]}

      # Got the majority
      has_quorum?(new_votes, fsm_data.cluster, [label: "Election"]) ->
        Logger.debug("[candidate] Got quorum !")
        {:next_state, :leader, fsm_data}

      # Not yet the majority, we wait
      true ->
        {:keep_state, fsm_data}
    end
  end

  # Ignore resp append
  def candidate(:cast, {:resp_append, {term, success?}, _from}, _fsm_data) do
    Logger.warn("[candidate] Got resp append with term #{term} - success #{success?}. Ignoring it !")
    :keep_state_and_data
  end

  def candidate(event_type, event_data, fsm_state) do
    handle_common(:candidate, event_type, event_data, fsm_state)
  end

  #########################################
  #              Leader logic             #
  #########################################

  def leader(:enter, _oldState, fsm_data) do
    Logger.debug("[leader] Entering leader state #{inspect(fsm_data, pretty: true)}")

    # Setup leader data
    next_log_index = (fsm_data[:logs] |> Enum.count) + 1
    nextIndex = fsm_data.cluster |> Enum.map(fn node_id ->
      {node_id, next_log_index}
    end) |> Enum.into(%{})

    matchIndex = fsm_data.cluster |> Enum.map(fn node_id ->
      {node_id, 0}
    end) |> Enum.into(%{})

    fsm_data = fsm_data
      |> Map.put(:nextIndex, nextIndex)
      |> Map.put(:matchIndex, matchIndex)
      |> Map.put(:replyMap, %{})
      |> Map.put(:leader, Node.self())
      |> Map.put(:firstBeatDone, false)
      |> Map.put(:validBeats, 0)

    # Append no-op entry
    fsm_data = add_log(new_entry(:no_op), nil, fsm_data)
    {:keep_state, fsm_data, [get_heartbeat_timer()]}
  end

  def leader({:timeout, :heartbeat}, :heartbeat, fsm_data) do
    # This is the end of the first beat
    # We check if first round got a quorum validity, else we go back to follower
    if not fsm_data.firstBeatDone and not has_quorum?(fsm_data.validBeats, fsm_data.cluster, [label: "Hearbeat"]) do
      Logger.debug("[leader] Invalid first round of heartbead, going back to follower")
      fsm_data = fsm_data |> Map.put(:leader, nil)
      {:next_state, :follower, fsm_data}
    else
      fsm_data = fsm_data
        |> Map.put(:firstBeatDone, true)
        |> Map.put(:validBeats, 0)
      send_entries(fsm_data)
      {:keep_state, fsm_data, [get_heartbeat_timer()]}
    end
  end

  def leader(:cast, {:resp_append, {term, success?}, from}, fsm_data) do
    Logger.warn("[leader] Got resp append with term #{term} - success #{success?} from #{from}")

    cond do
      # We got a better term elsewhere, stepping down
      term > fsm_data[:currentTerm] ->
        fsm_data = change_term(fsm_data, term)
        :ok = save_to_persistent_storage(fsm_data)
        Logger.debug("[leader] Invalid local term ! go back to follower")
        fsm_data = fsm_data |> Map.put(:leader, nil)
        {:next_state, :follower, fsm_data}

      # Write entry with success
      success? ->
        last_log_index = (fsm_data[:logs] |> Enum.count)
        new_nextIndex = fsm_data[:nextIndex]
          |> Map.put(from, last_log_index + 1)
        new_matchIndex = fsm_data[:matchIndex]
          |> Map.put(from, last_log_index)

        fsm_data = fsm_data
          |> Map.put(:nextIndex, new_nextIndex)
          |> Map.put(:matchIndex, new_matchIndex)
          |> Map.update(:validBeats, 1, fn current -> current + 1 end)

        fsm_data = update_commit_index(fsm_data)
        {:keep_state, fsm_data}

      # Failed to write entry, decrementing index to send
      true ->
        new_nextIndex = fsm_data[:nextIndex] |> Map.put(from, fsm_data[:nextIndex][from] - 1)
        fsm_data = fsm_data
          |> Map.put(:nextIndex, new_nextIndex)
          |> Map.update(:validBeats, 1, fn current -> current + 1 end)
        send_entries(fsm_data, from)
        {:keep_state, fsm_data}
    end
  end

  def leader({:call, from}, {:request_vote, req}, fsm_data) do
    Logger.debug("[leader] Received request vote #{inspect(req, [limit: :infinity, pretty: true])}")

    %{
      term: req_term,
      candidateId: candidate_id,
      lastLogIndex: last_c_log_idx,
      lastLogTerm: last_c_log_term
    } = req
    %{currentTerm: current_term, logs: logs} = fsm_data

    last_r_log = logs |> Enum.at(-1)
    last_r_log_idx = logs |> Enum.count
    last_r_log_term = last_r_log[:term] || -1

    give_vote? = cond do
      # Already voted
      not is_nil(fsm_data.votedFor) ->
        false
      # Leader does not have the most recent term
      req_term < current_term ->
        false
      true ->
        log_up_to_date?(last_c_log_idx, last_c_log_term, last_r_log_idx, last_r_log_term)
    end
    # Setup current term
    fsm_data = if req_term < current_term, do: change_term(fsm_data, req_term), else: fsm_data

    reply_data = {:ok, {fsm_data[:currentTerm], give_vote?}}
    if give_vote? do
      fsm_data = fsm_data |> Map.put(:votedFor, candidate_id)
      :ok = save_to_persistent_storage(fsm_data)
      fsm_data = fsm_data |> Map.put(:leader, nil)
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}]}
    else
      :ok = save_to_persistent_storage(fsm_data)
      {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
    end
  end

  # Ignore resp vote
  def leader(:cast, {:resp_vote, {_term, _vote?}, _from}, _fsm_data) do
    :keep_state_and_data
  end

  def leader({:call, from}, {:raft, req}, fsm_data) do
    Logger.warn("[leader] Received req for raft #{inspect req} from #{inspect from}")
    req = new_entry(req)
    fsm_data = add_log(req, from, fsm_data)
    {:keep_state, fsm_data}
  end

  def leader({:timeout, :election}, :election_timeout, _fsm_data) do
    Logger.debug("[leader] Received election timeout, ignore it")
    :keep_state_and_data
  end

  def leader(event_type, event_data, fsm_state) do
    handle_common(:leader, event_type, event_data, fsm_state)
  end

  #########################################
  #             Common events             #
  #########################################

  def handle_common(_state, {:call, from}, :get_leader, fsm_state) do
    {:keep_state_and_data, [{:reply, from, {:ok, fsm_state[:leader]}}]}
  end

  #########################################
  #               Core logic              #
  #########################################

  defp log_up_to_date?(last_c_log_idx, last_c_log_term, last_r_log_idx, last_r_log_term) do
    cond do
      # Same last term, but candidates has more or same logs
      last_r_log_term == last_c_log_term -> last_r_log_idx <= last_c_log_idx

      # Candidate is more up to date on terms
      true -> last_r_log_term < last_c_log_term
    end
  end

  defp add_log(new_log, call_from, fsm_data) do
    # Format log entries to incorporate currentTerm
    new_log = %{data: new_log, term: fsm_data[:currentTerm]}
    new_logs = fsm_data[:logs] ++ [new_log]
    last_index = new_logs |> Enum.count

    # Reply upon commit of this index by the cluster
    # to the node asking to process this data (node_from)
    new_replyMap = case call_from do
      nil ->
        fsm_data.replyMap
      _ ->
        Logger.debug("[ADD LOGS] Updating #{inspect fsm_data[:replyMap]} for #{last_index}")
        fsm_data[:replyMap] |> Map.update(last_index, [call_from], fn
          current_list -> current_list ++ [call_from]
        end)
    end

    # Add new log entries to local FSM state
    fsm_data = fsm_data
      |> Map.put(:logs, new_logs)
      |> Map.put(:replyMap, new_replyMap)

    :ok = save_to_persistent_storage(fsm_data)

    # Propagate changes to the cluster
    send_entries(fsm_data)
    fsm_data
  end

  # Update the commit index given the matchIndex
  defp update_commit_index(fsm_data) do
    Logger.warn("[COMMIT] Trying to update COMMIT INDEX")
    biggest_index = fsm_data[:matchIndex]
      |> Map.values
      |> Enum.group_by(& &1)
      |> Map.values
      |> Stream.map(fn l -> {Enum.count(l), Enum.at(l, 0)} end)
      |> Enum.sort(fn {s, _a}, {s2, _b} -> s > s2 end)
      |> Enum.at(0)
      |> elem(1)

    Logger.warn("[COMMIT] Biggest index is #{inspect biggest_index}")

    logs = fsm_data[:logs]
    log_at_biggest = if Enum.empty?(logs), do: nil, else: logs |> Enum.at(biggest_index - 1)
    Logger.warn("[COMMIT] log_at_biggest is #{inspect log_at_biggest}")

    term_at_biggest = log_at_biggest[:term] || -1

    Logger.warn("[COMMIT] term_at_biggest is #{inspect term_at_biggest}")
    cond do
      # # New setup
      # fsm_data[:commitIndex] == 0 && biggest_index >= fsm_data[:commitIndex] ->
      #   Logger.warn("NEW COMMIT INDEX is #{inspect biggest_index}")
      #   fsm_data = apply_logs(fsm_data, fsm_data[:commitIndex], biggest_index, logs)
      #   fsm_data |> Map.put(:commitIndex, biggest_index)

      # Running fsm
      biggest_index >= fsm_data[:commitIndex] && term_at_biggest == fsm_data[:currentTerm] ->
        Logger.warn("NEW COMMIT INDEX is #{inspect biggest_index}")
        fsm_data = fsm_data |> Map.put(:commitIndex, biggest_index)
        apply_logs(fsm_data, fsm_data.lastApplied, fsm_data.commitIndex, logs)
      true ->
        fsm_data
    end
  end

  # Commit the local logs given the new commit index on the leader
  # Commit the logs by calling the local FSM
  defp apply_logs(fsm_data, last_applied, to_commit, logs) do
    if last_applied >= to_commit do
      fsm_data
    else
      log_entry = logs |> Enum.fetch!(last_applied)

      Logger.debug("Applying log entry #{last_applied} #{inspect(log_entry, [limit: :infinity, pretty: true])} to the FSM #{inspect fsm_data.module}")

      {:raft, req} = log_entry.data
      {reply, fsm_data} = handle_raft_entry(req, last_applied, fsm_data)

      Logger.debug("#{last_applied} APPLIED")

      # Contact the waiting client(s)
      ## +1 because starts at 0, so real is last_applied +1
      to_notify = fsm_data[:replyMap][last_applied + 1] || []
      Logger.debug("NOTIFYING #{inspect last_applied}: #{inspect to_notify}")

      spawn fn ->
        to_notify |> Enum.each(fn from ->
          :gen_statem.reply(from, reply)
        end)
      end

      # Remove from reply map after notify
      new_reply_map = fsm_data[:replyMap] || %{}
      new_reply_map = Map.delete(new_reply_map, last_applied + 1)
      fsm_data = fsm_data
        |> Map.put(:replyMap, new_reply_map)
        |> Map.put(:lastApplied, last_applied + 1)

      apply_logs(fsm_data, fsm_data.lastApplied, to_commit, logs)
    end
  end

  # Propagate new FSM state logs to the cluster
  defp send_entries(fsm_data, target_node \\ :all)
  defp send_entries(fsm_data, :all) do
    fsm_data.cluster |> Enum.each(fn node_id ->
      send_entries(fsm_data, node_id)
    end)
  end

  defp send_entries(fsm_data, target_node) do
    node_nextIndex = fsm_data[:nextIndex][target_node]

    logs = fsm_data[:logs]
    last_log_idx = logs |> Enum.count

    Logger.debug("SEND_ENTRIES FOR #{target_node}: last_log #{last_log_idx}  node last log #{node_nextIndex}")

    if last_log_idx >= node_nextIndex do

      to_take = (last_log_idx - node_nextIndex) + 1
      entries = logs |> Enum.take(-to_take)

      prev_log_index = node_nextIndex - 1
      prev_log = if prev_log_index - 1 >= 0, do: logs |> Enum.at(prev_log_index - 1), else: nil
      prev_log_term = prev_log[:term] || -1

      Logger.debug("NEW LOG TO SEND TO #{target_node}: #{inspect to_take} : #{inspect entries}  #{inspect prev_log_index} #{inspect prev_log} #{inspect prev_log_term}")

      query = %{
        term: fsm_data[:currentTerm],
        leaderID: Node.self(),
        prevLogIndex: prev_log_index,
        prevLogTerm: prev_log_term,
        entries: entries,
        leaderCommit: fsm_data[:commitIndex]
      }
      Logger.debug("Sending send_entries to #{target_node} #{inspect(query, pretty: true)}")
      :rpc.cast(target_node, __MODULE__, :append_entries_rpc, [Node.self, query])
    else
      prev_log = if Enum.empty?(logs), do: nil, else: logs |> Enum.at(-1)
      prev_log_term = prev_log[:term] || -1
      query = %{
        term: fsm_data[:currentTerm],
        leaderID: Node.self(),
        prevLogIndex: last_log_idx,
        prevLogTerm: prev_log_term,
        entries: [],
        leaderCommit: fsm_data[:commitIndex]
      }
      Logger.debug("Sending send_entries to #{target_node} #{inspect(query, pretty: true)}")
      :rpc.cast(target_node, __MODULE__, :append_entries_rpc, [Node.self, query])
    end
  end

  defp change_term(fsm_data, new_term) do
    fsm_data
      |> Map.put(:currentTerm, new_term)
      |> Map.put(:votedFor, nil)
      |> Map.put(:votes, 0)
  end

  defp get_election_timer() do
    {{:timeout, :election}, Enum.random(2_000..3_000), :election_timeout}
  end

  defp get_heartbeat_timer() do
    {{:timeout, :heartbeat}, 1_000, :heartbeat}
  end

  defp has_quorum?(value, cluster, opts \\ [])
  defp has_quorum?(value, cluster, opts) do
    label = Keyword.get(opts, :label, "")
    Logger.debug("[Raft Protocol] Checking #{label} quorum: got #{value}, need #{((cluster |> Enum.count) / 2 + 1 )}")
    value >= ((cluster |> Enum.count) / 2 + 1 )
  end

  defp save_to_persistent_storage(fsm_data) do
    RocksDB.put(fsm_data.storage, @state_key, %{
      currentTerm: fsm_data.currentTerm,
      votedFor: fsm_data.votedFor,
      logs: fsm_data.logs,
    }, [sync: true])
  end

  #########################################
  #          FSM Inner Workings           #
  #########################################

  defp new_entry(req) do
    {:raft, req}
  end

  defp handle_raft_entry(:no_op, _log_idx, fsm_data), do: {:ok, fsm_data}

  # Allocate a session for a new client
  defp handle_raft_entry(:register_client, log_idx, fsm_data) do

    case fsm_data.clients[log_idx] do
      nil ->
        Logger.debug("[Raft Protocol] New client registered with id #{inspect log_idx}")
        {{:ok, log_idx}, fsm_data |> put_in([:clients, log_idx], %{})}
      _ ->
        raise "Unable to register client for #{inspect log_idx}. Already exists !"
    end
  end

  defp handle_raft_entry({client_id, sequence_num, {:write_log, new_log}}, _log_idx, fsm_data) do
    Logger.warn("[Raft Protocol] Received write log with #{inspect(new_log)} from client #{inspect client_id}")

    case fsm_data.clients[client_id] do
      # No such client in base
      nil ->
        {:session_expired, fsm_data}
      client_data ->
        case client_data[sequence_num] do
          # Need to process
          nil ->
            #Apply the log to the FSM by calling the function :apply_log_entry/2 from the FSM Elixir Module
            reply = apply(fsm_data.module, :apply_log_entry, [new_log, fsm_data])

            # Save answer for this specific sequence
            # TODO disgard all previous sequences
            fsm_data = fsm_data |> put_in([:clients, client_id, sequence_num], reply)

            {reply, fsm_data}
          # No need to process, answer directly
          reply ->
            {reply, fsm_data}
        end
    end
  end

  defp handle_raft_entry(req, _log_idx, _fsm_data) do
    raise "Unknown raft entry #{inspect req}"
  end
end
