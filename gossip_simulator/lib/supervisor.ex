defmodule GS do

  def main do
    # Take input arguments
    GS.Supervisor.main()
    # listen()

  end

  def listen do
    receive do
      {:exit}  -> nil
    end
  end

end

defmodule GS.Supervisor do
  @moduledoc """
  Documentation for GS.
  """
  use Supervisor

  def main do
    # n -> Number of actors to be generated
    n = 15
    start = 1
    {:ok, super_pid} = start_link(n)
    worker_ids = get_worker_ids(%{}, Supervisor.which_children(super_pid), start)
    send_neighbors(n, worker_ids)
    start = :rand.uniform(n)
    starter_pid = Map.get(worker_ids, start)
    Process.send(starter_pid, {:receive_gossip, "message"}, [])
    state = listen(n)
    IO.inspect state
  end

  def start_link(n) do
    children = create_workers(n, [], n)
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def init([]) do
    nil
  end

  def listen(n) do
    receive do
      {:received, pid} ->
        n = n - 1
        if n > 0 do
          listen(n)
        end
    end
  end

  def create_workers(n, workers, neighbors) do
    if n > 0 do
      workers = [
        %{
          id: n,
          start: {GS.Worker, :start_link, [%{:id => n, :count => 0, :parent => self(), :num_neighbors => neighbors}]}
        }
        | workers]

      create_workers(n-1, workers, neighbors)
    else
      workers
    end
  end

  def get_worker_ids(worker_ids, [child | children], idx) do
    {_, id, _, _} = child
    worker_ids = Map.put(worker_ids, idx, id)
    get_worker_ids(worker_ids, children, idx + 1)
  end

  def get_worker_ids(worker_ids, [], idx) do
    worker_ids
  end

  def send_neighbors(n, worker_ids) do
    if n > 0 do
      worker_id = Map.get(worker_ids, n)
      Process.send(worker_id, {:add_neighbors, worker_ids}, [])
      send_neighbors(n-1, worker_ids)
    end
  end
end

defmodule GS.Worker do
  use GenServer

  def start_link(status) do
    GenServer.start_link(__MODULE__, status)
  end

  def init(args) do
    {:ok, args}
  end

  # Callbacks

  def handle_info({tag, message}, state) do
    case tag do
      :add_neighbors -> add_neighbors(state, message)
      :receive_gossip -> receive_gossip(state, message)
      :send_gossip -> send_gossip(state, message)
    end
  end

  # private functions

  defp add_neighbors(state, message) do
    # TODO: modify the neighbors based upon the topology
    idx = Map.get(state, :id)
    new_state = Map.put(state, :neighbors, message)

    {:noreply, new_state}
  end

  defp send_gossip(state, message) do
    count = Map.get(state, :count)

    if count < 10 do
      num_neighbors = Map.get(state, :num_neighbors)
      self_idx = Map.get(state, :id)
      idx = get_neighbor_idx(self_idx, num_neighbors)
      neighbor = get_in(state, [:neighbors, idx])
      Process.send(neighbor, {:receive_gossip, message}, [])

      #call yourself once in every 100 ms
      Process.send_after(self(), {:send_gossip, message}, 100)
    end

    {:noreply, state}
  end

  defp receive_gossip(state, message) do
    count = Map.get(state, :count)
    idx = Map.get(state, :id)

    cond do
      count == 0 ->
        parent_pid = Map.get(state, :parent)
        Process.send(parent_pid, {:received, self()}, [])
        Process.send(self(), {:send_gossip, message}, [])
        new_state = Map.update!(state, :count, &(&1 + 1))
        {:noreply, new_state}
      count < 10 ->
        new_state = Map.update!(state, :count, &(&1 + 1))
        {:noreply, new_state}
      true -> {:noreply, state}
    end
  end

  defp get_neighbor_idx(self_idx, num_neighbors) do
    idx = :rand.uniform(num_neighbors)
    if idx == self_idx do
      get_neighbor_idx(self_idx, num_neighbors)
    else
      idx
    end
  end

end

GS.main()
