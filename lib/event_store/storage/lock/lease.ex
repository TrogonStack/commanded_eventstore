defmodule EventStore.Storage.Lock.Lease do
  @moduledoc false

  alias EventStore.Storage.Session
  alias EventStore.Subscriptions.FencingToken

  @type t :: %__MODULE__{
          token: FencingToken.t(),
          last_seen: non_neg_integer() | nil,
          session: Session.t()
        }

  @enforce_keys [:token, :last_seen, :session]
  defstruct [:token, :last_seen, :session]

  def new(%FencingToken{} = token, last_seen, %Session{} = session) do
    %__MODULE__{token: token, last_seen: last_seen, session: session}
  end
end
