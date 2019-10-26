defmodule Raft.Application do
  @moduledoc """
  Raft Application module
  """

  use Application

  def start(_type, _args) do
    # children = [
    #   Raft.LocalStorage,
    #   Raft.Protocol
    # ]
    # Supervisor.start_link(children, [strategy: :one_for_one])
    Raft.Protocol.start_link
  end
end
