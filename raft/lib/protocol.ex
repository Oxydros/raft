defmodule Raft.Protocol do
  @moduledoc """
  :gen_statem FSM implementing the Raft procotol
  """

  require Logger

  def id(), do: :toto
  def cluster, do: []

  ## CLIENT

  def append_entries(asking_node, req) do
    {:ok, {_term, _success}=res} = :gen_statem.call({:local, __MODULE__}, {:append_entries, req})
    :gen_statem.cast({__MODULE__, asking_node}, {:resp_append, res})
    :ok
  end

  def request_vote(asking_node, req) do
    {:ok, {_term, _vote}=res} = :gen_statem.call({:local, __MODULE__}, {:request_vote, req})
    :gen_statem.cast({__MODULE__, asking_node}, {:resp_vote, res})
    :ok
  end

  ## SERVER

  def start_link(), do: :gen_statem.start_link({:local, __MODULE__}, __MODULE__, nil, [])
  def stop(), do: :gen_statem.stop(__MODULE__)

  def callback_mode() do
    [:state_functions, :state_enter]
  end

  def init() do
    initial_state = %{
      # Persistent on storage
      currentTerm: 0,
      votedFor: nil,
      log: [],

      # Volatile -- all
      commitIndex: 0,
      lastApplied: 0,

      # Volatile -- candidates
      votes: 0,

      # Volatile -- leaders
      nextIndex: [],
      matchIndex: []
    }

    {:ok, :follower, initial_state, [get_election_timer()]}
  end

  # def terminate(_reason, _state, _data) do
  #   :ok
  # end

  ## FOLLOWER LOGIC

  def follower(:enter, _oldState, fsm_data) do
    Logger.debug("[follower] Entering follower state")
    {:keep_state, fsm_data, [get_election_timer()]}
  end

  def follower({:timeout, :election}, :election_timeout, fsm_data) do
    Logger.debug("[follower] Election timed-out ! Becoming a candidate")
    {:next_state, :candidate, fsm_data}
  end

  def follower({:call, from}, {:append_entries, req}, fsm_data) do
    Logger.debug("[follower] Received new entries. Reset timeout")

    %{
      term: l_term,
      leaderID: l_id,
      prevLogIndex: l_prev_idx,
      prevLogTerm: l_prev_term,
      entries: entries,
      leaderCommit: l_commit_idx
    } = req

    log_term_at_idx = fsm_data[:logs] |> Enum.at(l_prev_idx - 1) |> Map.get(:term)
    failure? = l_term < fsm_data[:currentTerm] || log_term_at_idx != l_prev_term

    fsm_data = if not failure? do
      # Take all logs we agree upon, and add the new entries
      # This will overwrite eventual conflicts
      new_logs = (fsm_data[:logs] |> Enum.take(l_prev_idx)) ++ entries
      new_commit_idx = min(l_commit_idx, new_logs |> Enum.count)
      new_last_applied = apply_logs(new_logs, fsm_data[:lastApplied], new_commit_idx)
      fsm_data
        |> Map.put(:logs, new_logs)
        |> Map.put(:commitIndex, new_commit_idx)
        |> Map.put(:lastApplied, new_last_applied)
        |> Map.put(:currentTerm, l_term)
    else
      fsm_data
    end

    # Setup current term
    fsm_data = if l_term < fsm_data[:currentTerm], do: change_term(fsm_data, l_term), else: fsm_data

    :ok = Raft.LocalStorage.save_data("protocol_state", fsm_data)
    reply_data = {:ok, {fsm_data[:currentTerm], not failure?}}
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  def follower({:call, from}, {:request_vote, req}, %{currentTerm: current_term}=fsm_data) do
    Logger.debug("[follower] Received request vote")

    %{
      term: req_term,
      candidateId: candidate_id,
      lastLogIndex: last_c_log_idx,
      lastLogTerm: last_c_log_term
    } = req
    %{currentTerm: current_term, votedFor: vote, log: logs} = fsm_data

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

    :ok = Raft.LocalStorage.save_data("protocol_state", fsm_data)
    reply_data = {:ok, {current_term, give_vote?}}
    {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
  end

  # Ignore resp vote
  def follower(:cast, {:resp_vote, {_term, _vote?}}, _fsm_data) do
    :keep_state_and_data
  end

  ## --- CANDIDATE LOGIC ---

  def candidate(:enter, _oldState, fsm_data) do
    Logger.debug("[candidate] Entering candidate state")

    # Start a new election
    new_term = fsm_data[:currentTerm] + 1

    fsm_data = fsm_data
      |> Map.put(:currentTerm, new_term)
      |> Map.put(:vote, id())

    last_log = fsm_data[:logs] |> Enum.at(-1)
    last_log_idx = fsm_data[:logs] |> Enum.count
    last_log_term = last_log[:term] || -1

    query = %{
      term: new_term,
      candidateId: id(),
      lastLogIndex: last_log_idx,
      lastLogTerm: last_log_term
    }

    :rpc.eval_everywhere(cluster(), __MODULE__, :request_vote, [Node.self, query])
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
      fsm_data = fsm_data |> change_term(l_term)
      reply_data = {:ok, {fsm_data[:currentTerm], true}}
      {:next_state, :follower, fsm_data, [{:reply, from, reply_data}, :postpone]}
    else
      # I'm better than the leader, refuse the append entry
      reply_data = {:ok, {fsm_data[:currentTerm], false}}
      {:keep_state, fsm_data, [{:reply, from, reply_data}, get_election_timer()]}
    end
  end

  def candidate({:call, from}, {:request_vote, req}, %{currentTerm: current_term}=fsm_data) do
    Logger.debug("[candidate] Received request vote")

    %{
      term: req_term,
      candidateId: candidate_id,
      lastLogIndex: last_c_log_idx,
      lastLogTerm: last_c_log_term
    } = req
    %{currentTerm: current_term, log: logs} = fsm_data

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

  def candidate(:cast, {:resp_vote, {term, vote?}}, %{currentTerm: current_term, votes: votes}=fsm_data) do
    new_votes = if vote?, do: votes + 1, else: votes
    fsm_data = fsm_data |> Map.put(:votes, new_votes)

    cond do
      # Receive response with better term
      # Going back to follower
      term > current_term ->
        fsm_data = change_term(fsm_data, term)
        {:next_state, :follower, fsm_data, [get_election_timer()]}

      # Got the majority
      has_quorum?(new_votes, cluster()) ->
        {:next_state, :leader, fsm_data}

      # Not yet the majority, we wait
      true ->
        {:keep_state, fsm_data}
    end
  end

  ## LEADER LOGIC

  def leader(:enter, _oldState, fsm_data) do
    Logger.debug("[leader] Entering leader state")
    send_entries([], fsm_data)
    {:keep_state, fsm_data, [get_heartbeat_timer()]}
  end

  def leader({:timeout, :heartbeat}, :heartbeat, fsm_data) do
    send_entries([], fsm_data)
    {:keep_state, fsm_data, [get_heartbeat_timer()]}
  end

  # Ignore resp vote
  def leader(:cast, {:resp_append, {term, vote?}}, fsm_data) do
    :keep_state_and_data
  end

  # Ignore resp vote
  def leader(:cast, {:resp_vote, {_term, _vote?}}, _fsm_data) do
    :keep_state_and_data
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

  defp apply_logs(logs, last_applied, commit_idx) do
    if commit_idx > last_applied do
      logs_size = logs |> Enum.count
      new_last_applied = last_applied + 1
      log_entry = logs |> Enum.fetch!(new_last_applied)
      Logger.debug("Applying log entry #{new_last_applied} #{inspect(log_entry, [limit: :infinity, pretty: true])} to the FSM")
      apply_logs(logs, new_last_applied, commit_idx)
    else
      last_applied
    end
  end

  defp send_entries(entries, fsm_data) do
    last_log = fsm_data[:logs] |> Enum.at(-1)
    last_log_idx = fsm_data[:logs] |> Enum.count
    last_log_term = last_log[:term] || -1

    query = %{
      term: fsm_data[:currentTerm],
      leaderID: id(),
      prevLogIndex: last_log_idx,
      prevLogTerm: last_log_term,
      entries: entries,
      leaderCommit: fsm_data[:commitIndex]
    }
    :rpc.eval_everywhere(cluster(), __MODULE__, :append_entries, [Node.self, query])
  end

  defp change_term(fsm_data, new_term) do
    fsm_data
      |> Map.put(:currentTerm, new_term)
      |> Map.put(:vote, nil)
      |> Map.put(:votes, 0)
  end

  defp get_election_timer() do
    {{:timeout, :election}, Enum.random(150..350), :election_timeout}
  end

  defp get_heartbeat_timer() do
    {{:timeout, :heartbeat}, 50, :heartbeat}
  end

  defp has_quorum?(vote_number, cluster) do
    vote_number > ((cluster |> Enum.count) / 2 + 1 )
  end
end
