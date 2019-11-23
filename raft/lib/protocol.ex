defmodule Raft.Protocol do
  @moduledoc """
  :gen_statem FSM implementing the Raft procotol
  """

  require Logger

  def cluster, do: [:toto@nameless, :titi@nameless, :tata@nameless] |> Enum.reject(fn n -> n == Node.self() end)

  ## CLIENT -- FSM

  @doc """
  Write a log on the distributed FSM, and apply it.
  """
  def write_log(new_log) when not is_list(new_log), do: write_log([new_log])
  def write_log(new_log) do
    leader = get_leader()
    cond do
      leader == Node.self ->
        ret_ref = make_ref()
        :ok = :gen_statem.cast(__MODULE__, {:write_log, new_log, self(), ret_ref})
        receive do
          {^ret_ref, reply} ->
            {:ok, reply}
        after
          10_000 ->
            {:error, :timeout}
        end
      true ->
        {:error, {:redirect, leader}}
    end
  end

  ## SERVER COMMUNICATION

  def start_fsm() do
    :ok = :gen_statem.cast(__MODULE__, :begin)
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

  def get_leader() do
    {:ok, leader} = :gen_statem.call(__MODULE__, :get_leader)
    leader
  end

  ## SERVER

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      restart: :transient,
      type: :worker
    }
  end

  def start_link(), do: :gen_statem.start_link({:local, __MODULE__}, __MODULE__, nil, [])
  def stop(), do: :gen_statem.stop(__MODULE__)

  def callback_mode() do
    [:state_functions, :state_enter]
  end

  def init(_) do
    initial_state = %{
      # Persistent on storage
      currentTerm: 0,
      votedFor: nil,
      logs: [],

      # Volatile -- all
      commitIndex: 0,
      leader: nil,

      # Volatile -- candidates
      votes: 0,

      # Volatile -- leaders
      nextIndex: [],
      matchIndex: [],
      replyMap: %{}
    }

    Logger.debug("Starting FSM for #{Node.self()}")

    ## Init reading local storage
    ## TODO

    {:ok, :follower, initial_state, []}
  end

  # def terminate(_reason, _state, _data) do
  #   :ok
  # end

  def stand_by(:enter, _oldState, fsm_data) do
    Logger.debug("[stand by] Entering init state #{inspect(fsm_data, pretty: true)}")
    :keep_state_and_data
  end

  # Await all cluster to be up before launching the FSM
  def stand_by(:cast, :begin, fsm_data)do
    Logger.debug("[stand by] Receive BEGIN notif. Launching Protocol.")
    {:next_state, :follower, fsm_data}
  end

  ## FOLLOWER LOGIC

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

    log_term_at_idx = fsm_data[:logs] |> Enum.at(l_prev_idx - 1)
    log_term_at_idx = log_term_at_idx[:term] || -1

    # We dont agree upon the version of logs, or follower have a better term
    failure? = l_term < fsm_data[:currentTerm] || log_term_at_idx != l_prev_term

    fsm_data = if not failure? do
      # Take all logs we agree upon, and add the new entries
      # This will overwrite eventual conflicts
      new_logs = (fsm_data[:logs] |> Enum.take(l_prev_idx)) ++ entries
      new_commit_idx = min(l_commit_idx, new_logs |> Enum.count)

      #Apply logs from fsm_data[:commitIndex] to new_commit_idx
      fsm_data = apply_logs(fsm_data, fsm_data[:commitIndex], new_commit_idx, new_logs)
      fsm_data
        |> Map.put(:logs, new_logs)
        |> Map.put(:commitIndex, new_commit_idx)
        |> Map.put(:currentTerm, l_term)
        |> Map.put(:leader, l_id)
    else
      fsm_data
    end

    # Setup current term
    fsm_data = if l_term < fsm_data[:currentTerm], do: change_term(fsm_data, l_term), else: fsm_data

    # :ok = Raft.LocalStorage.save_data("protocol_state", fsm_data)
    reply_data = {:ok, {fsm_data[:currentTerm], not failure?}}
    Logger.warn("[follower] Replying #{inspect(reply_data, pretty: true)}")
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  def follower({:call, from}, {:request_vote, req}, %{currentTerm: current_term}=fsm_data) do
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
        |> Map.put(:vote, candidate_id)
    else
      fsm_data
    end

    # :ok = Raft.LocalStorage.save_data("protocol_state", fsm_data)
    reply_data = {:ok, {current_term, give_vote?}}
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  # Ignore resp vote
  def follower(:cast, {:resp_vote, {_term, _vote?}, _from}, _fsm_data) do
    :keep_state_and_data
  end

  def follower(event_type, event_data, fsm_state) do
    handle_common(:follower, event_type, event_data, fsm_state)
  end

  ## --- CANDIDATE LOGIC ---

  def candidate(:enter, _oldState, fsm_data) do
    Logger.debug("[candidate] Entering candidate state #{inspect(fsm_data, pretty: true)}")

    # Start a new election
    new_term = fsm_data[:currentTerm] + 1

    fsm_data = fsm_data
      |> Map.put(:currentTerm, new_term)
      |> Map.put(:vote, Node.self())
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

    :rpc.eval_everywhere(cluster(), __MODULE__, :request_vote_rpc, [Node.self, query])
    {:keep_state, fsm_data, [get_election_timer()]}
  end

  def candidate({:timeout, :election}, :election_timeout, fsm_data) do
    Logger.debug("[candidate] Election timed-out ! Start a new election")
    :repeat_state_and_data
  end

  def candidate({:call, from}, {:append_entries, req}, fsm_data) do
    Logger.debug("[candidate] Received new entries")
    %{
      term: l_term,
      leaderID: l_id,
      prevLogIndex: l_prev_idx,
      prevLogTerm: l_prev_term,
      entries: entries,
      leaderCommit: l_commit_idx
    } = req
    if l_term > fsm_data[:currentTerm] do
      # Accept entry, go back as follower
      fsm_data = fsm_data
        |> change_term(l_term)
        |> Map.put(:leader, l_id)
      reply_data = {:ok, {fsm_data[:currentTerm], true}}
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}, :postpone]}
    else
      # I'm better than the leader, refuse the append entry
      reply_data = {:ok, {fsm_data[:currentTerm], false}}
      {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
    end
  end

  def candidate({:call, from}, {:request_vote, req}, %{currentTerm: current_term}=fsm_data) do
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
      fsm_data = fsm_data |> Map.put(:vote, candidate_id)
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}]}
    else
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
        {:next_state, :follower, fsm_data, [get_election_timer()]}

      # Got the majority
      has_quorum?(new_votes, cluster()) ->
        Logger.debug("[candidate] Got quorum !")
        {:next_state, :leader, fsm_data}

      # Not yet the majority, we wait
      true ->
        {:keep_state, fsm_data}
    end
  end

  # Ignore resp vote
  def candidate(:cast, {:resp_append, {term, success?}, _from}, fsm_data) do
    Logger.warn("[candidate] Got resp append with term #{term} - success #{success?}. Ignoring it !")
    :keep_state_and_data
  end

  def candidate(event_type, event_data, fsm_state) do
    handle_common(:candidate, event_type, event_data, fsm_state)
  end

  ## LEADER LOGIC

  def leader(:enter, _oldState, fsm_data) do
    Logger.debug("[leader] Entering leader state #{inspect(fsm_data, pretty: true)}")
    send_entries(fsm_data)

    # Setup leader data
    next_log_index = (fsm_data[:logs] |> Enum.count) + 1
    nextIndex = cluster() |> Enum.map(fn node_id ->
      {node_id, next_log_index}
    end) |> Enum.into(%{})

    matchIndex = cluster() |> Enum.map(fn node_id ->
      {node_id, 0}
    end) |> Enum.into(%{})

    fsm_data = fsm_data
      |> Map.put(:nextIndex, nextIndex)
      |> Map.put(:matchIndex, matchIndex)
      |> Map.put(:replyMap, %{})
      |> Map.put(:leader, Node.self())
    {:keep_state, fsm_data, [get_heartbeat_timer()]}
  end

  def leader({:timeout, :heartbeat}, :heartbeat, fsm_data) do
    send_entries(fsm_data)
    {:keep_state, fsm_data, [get_heartbeat_timer()]}
  end

  def leader(:cast, {:resp_append, {term, success?}, from}, fsm_data) do
    Logger.warn("[leader] Got resp append with term #{term} - success #{success?} from #{from}")

    cond do
      # We got a better term elsewhere, stepping down
      term > fsm_data[:currentTerm] ->
        fsm_data = change_term(fsm_data, term)
        Logger.debug("[leader] Invalid local term ! go back to follower")
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

        fsm_data = update_commit_index(fsm_data)
        {:keep_state, fsm_data}

      # Failed to write entry, decrementing index to send
      true ->
        new_nextIndex = fsm_data[:nextIndex] |> Map.put(from, fsm_data[:nextIndex][from] - 1)
        fsm_data = fsm_data |> Map.put(:nextIndex, new_nextIndex)
        send_entries(fsm_data, from)
        {:keep_state, fsm_data}
    end
  end

  # Ignore resp vote
  def leader(:cast, {:resp_vote, {_term, _vote?}, _from}, _fsm_data) do
    :keep_state_and_data
  end

  def leader(:cast, {:write_log, new_logs, from, ret_ref}, fsm_state) do
    Logger.warn("[leader] Received write log with #{inspect(new_logs)}")
    fsm_state = add_logs(new_logs, from, ret_ref, fsm_state)
    {:keep_state, fsm_state}
  end

  def leader({:timeout, :election}, :election_timeout, fsm_data) do
    Logger.debug("[leader] Received election timeout, ignore it")
    :keep_state_and_data
  end

  def leader(event_type, event_data, fsm_state) do
    handle_common(:leader, event_type, event_data, fsm_state)
  end

  ## -- COMMON EVENTS --

  def handle_common(_state, {:call, from}, :get_leader, fsm_state) do
    {:keep_state_and_data, [{:reply, from, {:ok, fsm_state[:leader]}}]}
  end

  ## -- CORE LOGIC --

  defp log_up_to_date?(last_c_log_idx, last_c_log_term, last_r_log_idx, last_r_log_term) do
    cond do
      # Same last term, but candidates has more or same logs
      last_r_log_term == last_c_log_term -> last_r_log_idx <= last_c_log_idx

      # Candidate is more up to date on terms
      true -> last_r_log_term < last_c_log_term
    end
  end

  defp add_logs(new_logs, node_from, ret_ref, fsm_state) do
    # Format log entries to incorporate currentTerm
    new_logs = new_logs |> Enum.map(fn log_data ->
      %{data: log_data, term: fsm_state[:currentTerm]}
    end)
    new_logs = fsm_state[:logs] ++ new_logs
    last_index = new_logs |> Enum.count

    # Reply upon commit of this index by the cluster
    # to the node asking to process this data (node_from)
    Logger.debug("[ADD LOGS] Updating #{inspect fsm_state[:replyMap]} for #{last_index}")
    new_replyMap = fsm_state[:replyMap] |> Map.update(last_index, [{node_from, ret_ref}], fn
        current_list -> current_list ++ [{node_from, ret_ref}]
      end)

    # Add new log entries to local FSM state
    fsm_state = fsm_state
      |> Map.put(:logs, new_logs)
      |> Map.put(:replyMap, new_replyMap)

    # Propagate changes to the cluster
    send_entries(fsm_state)
    fsm_state
  end

  # Update the commit index given the matchIndex
  defp update_commit_index(fsm_data) do
    Logger.warn("[COMMIT] Trying to update COMMIT INDEX")
    biggest_index = fsm_data[:matchIndex]
      |> Map.values
      |> Enum.group_by(& &1)
      |> Map.values
      |> Enum.map(fn l -> {Enum.count(l), Enum.at(l, 0)} end)
      |> Enum.sort(fn {s, _a}, {s2, _b} -> s > s2 end)
      |> Enum.at(0)
      |> elem(1)

    Logger.warn("[COMMIT] Biggest index is #{inspect biggest_index}")

    logs = fsm_data[:logs]
    log_at_biggest = if Enum.empty?(logs), do: nil, else: logs |> Enum.at(biggest_index - 1)
    Logger.warn("[COMMIT] log_at_biggest is #{inspect log_at_biggest}")

    term_at_biggest = log_at_biggest[:term] || -1

    Logger.warn("[COMMIT] term_at_biggest is #{inspect term_at_biggest}")
    if biggest_index > fsm_data[:commitIndex] && term_at_biggest == fsm_data[:currentTerm] do
      Logger.warn("NEW COMMIT INDEX is #{inspect biggest_index}")
      fsm_data = apply_logs(fsm_data, fsm_data[:commitIndex], biggest_index, logs)
      fsm_data |> Map.put(:commitIndex, biggest_index)
    else
      fsm_data
    end
  end

  # Commit the local logs given the new commit index on the leader
  # Commit the logs by calling the local FSM
  defp apply_logs(fsm_data, from_commit, to_commit, logs) do
    if from_commit == to_commit do
      fsm_data
    else
      log_entry = logs |> Enum.fetch!(from_commit)

      Logger.debug("Applying log entry #{from_commit} #{inspect(log_entry, [limit: :infinity, pretty: true])} to the FSM")
      #APPLY HERE
      last_res = "APPLIED YES"

      # Contact the waiting client(s)
      to_notify = fsm_data[:replyMap][from_commit + 1] || []
      Logger.debug("NOTIFYING #{inspect from_commit}: #{inspect to_notify}")
      to_notify |> Enum.each(fn {node_from, ret_ref} ->
        send(node_from, {ret_ref, last_res})
      end)

      fsm_data = fsm_data |> put_in([:replyMap, from_commit + 1], [])

      apply_logs(fsm_data, from_commit + 1, to_commit, logs)
    end
  end

  # Propagate new FSM state logs to the cluster
  defp send_entries(fsm_data, target_node \\ :all)
  defp send_entries(fsm_data, :all) do
    cluster() |> Enum.each(fn node_id ->
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
      |> Map.put(:vote, nil)
      |> Map.put(:votes, 0)
  end

  defp get_election_timer() do
    {{:timeout, :election}, Enum.random(2_000..3_000), :election_timeout}
  end

  defp get_heartbeat_timer() do
    {{:timeout, :heartbeat}, 1_000, :heartbeat}
  end

  defp has_quorum?(vote_number, cluster) do
    Logger.debug("[candidate] Checking quorum: got #{vote_number}, need #{((cluster |> Enum.count) / 2 + 1 )}")
    vote_number >= ((cluster |> Enum.count) / 2 + 1 )
  end
end
