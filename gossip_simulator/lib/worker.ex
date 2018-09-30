defmodule GS.Worker do
  use GenServer

  def start_link(status) do
    GenServer.start_link(__MODULE__, status)
  end

  def init(args) do
    {:ok, args}
  end

end
