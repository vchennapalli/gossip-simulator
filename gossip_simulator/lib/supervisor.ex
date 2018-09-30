defmodule GS do

  def main do
    # Take input arguments

    [num_nodes, topology, algorithm] = System.argv()
    num_nodes = String.to_integer(num_nodes)
    GS.Supervisor.main(num_nodes, topology, algorithm)

  end

  # def listen do
  #   receive do
  #     {:exit}  -> nil
  #   end
  # end

end

defmodule GS.Supervisor do
  @moduledoc """
  Documentation for GS.
  """
  use Supervisor

  def main(num_nodes, topology, _) do
    initialize(num_nodes, topology)

    listen_to_children(num_nodes)
  end

  def initialize(num_nodes, topology) do
    start = 1
    {:ok, super_pid} = start_link(num_nodes)
    worker_ids = get_worker_ids(%{}, Supervisor.which_children(super_pid), start)
    send_neighbors(num_nodes, worker_ids)

    begin_gossip(num_nodes, worker_ids)
  end

  def begin_gossip(num_nodes, worker_ids) do
    start = :rand.uniform(num_nodes)
    starter_pid = Map.get(worker_ids, start)
    Process.send(starter_pid, {:receive_gossip, "message"}, [])
  end

  def start_link(num_children) do
    children = create_workers(num_children, [], num_children)
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def init([]) do
    nil
  end

  def listen_to_children(n) do
    receive do
      {:received} ->
        n = n - 1
        IO.puts n
        if n > 0 do
          listen_to_children(n)
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

  def get_worker_ids(worker_ids, [], _) do
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
    # idx = Map.get(state, :id)

    cond do
      count == 0 ->
        parent_pid = Map.get(state, :parent)
        Process.send(parent_pid, {:received}, [])
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
