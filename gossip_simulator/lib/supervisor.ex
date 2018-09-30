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
    n = 10000
    start = 1
    {:ok, super_pid} = start_link(n)
    worker_ids = get_worker_ids(%{}, Supervisor.which_children(super_pid), start)
    send_neighbors(n, worker_ids)
    start = :rand.uniform(n)
    starter_pid = Map.get(worker_ids, start)
    Process.send(starter_pid, {:gossip, "message"}, [])
    state = listen(n)
    # :timer.sleep(1000)
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
    # IO.puts n
    receive do
      {:received, pid} ->
        n = n - 1
        # IO.puts n
        # IO.puts n
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
    else
      # IO.puts "EXIT"
      # {:exit}
    end
  end

end

defmodule GS.Worker do
  use GenServer

  def start_link(status) do
    GenServer.start_link(__MODULE__, status)
  end

  def init(args) do
    # IO.inspect args
    {:ok, args}
  end

  # Callbacks

  def handle_info({tag, message}, state) do

    case tag do
      :add_neighbors ->
        # TODO: modify the neighbors based upon the topology
        idx = Map.get(state, :id)
        new_state = Map.put(state, :neighbors, message)
        # IO.inspect new_state
        {:noreply, new_state}

      :gossip ->
        count = Map.get(state, :count)

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

      :send_gossip ->
        count = Map.get(state, :count)

        if count < 10 do
          num_neighbors = Map.get(state, :num_neighbors)
          self_idx = Map.get(state, :id)
          idx = get_neighbor_idx(self_idx, num_neighbors)
          idx = :rand.uniform(num_neighbors)
          neighbor = get_in(state, [:neighbors, idx])
          # IO.inspect neighbor
          Process.send(neighbor, {:gossip, message}, [])

          #call yourself once in every 100 ms
          Process.send_after(self(), {:send_gossip, message}, 500)
        end

        {:noreply, state}
    end
  end

  def get_neighbor_idx(self_idx, num_neighbors) do
    idx = :rand.uniform(num_neighbors)
    if idx == self_idx do
      get_neighbor_idx(self_idx, num_neighbors)
    else
      idx
    end
  end

end

GS.main()
