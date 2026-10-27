defmodule EventStore.StreamReadError do
  @moduledoc """
  Raised when a batch of a lazily read stream cannot be read from storage.
  """

  defexception [:stream_uuid, :direction, :reason]

  @impl true
  def message(%__MODULE__{} = error) do
    %__MODULE__{stream_uuid: stream_uuid, direction: direction, reason: reason} = error

    "failed to read stream #{inspect(stream_uuid)} #{direction}: " <>
      if(is_binary(reason), do: reason, else: inspect(reason))
  end
end
