defmodule EventStore.Subscriptions.FencingToken do
  @moduledoc false

  @type t :: %__MODULE__{
          subscription_id: pos_integer(),
          generation: pos_integer()
        }

  @enforce_keys [:subscription_id, :generation]
  defstruct [:subscription_id, :generation]

  def new(subscription_id, generation)
      when is_integer(subscription_id) and is_integer(generation) and generation > 0 do
    %__MODULE__{subscription_id: subscription_id, generation: generation}
  end
end
