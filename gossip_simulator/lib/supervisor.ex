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

  def main(num_nodes, topology, algorithm) do
    initialize(num_nodes, topology, algorithm)

    if algorithm == "gossip" do
      hear_children_gossip(num_nodes)
    else
      cpc = 0.1  # convergence percentage condition
      threshold = round(num_nodes * cpc)
      _ratio = hear_children_sum(num_nodes, threshold)
    end

  end

  def initialize(num_nodes, topology, algorithm) do
    start = num_nodes
    {:ok, super_pid} = start_link(num_nodes, topology, algorithm)
    worker_ids = get_worker_ids(%{}, Supervisor.which_children(super_pid), start)
    send_neighbors(num_nodes, worker_ids)
    are_children_ready(num_nodes)
    begin_gossip(num_nodes, worker_ids, algorithm)
  end

  def begin_gossip(num_nodes, worker_ids, algorithm) do
    start = :rand.uniform(num_nodes)
    starter_pid = Map.get(worker_ids, start)

    if algorithm == "gossip" do
      Process.send(starter_pid, {:receive_gossip, "message"}, [])
    else
      Process.send(starter_pid, {:push_sum, {0, 0}}, [])
    end

  end

  def start_link(num_children, topology, algorithm) do
    children = create_workers(num_children, [], num_children, topology, algorithm)
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def init([]) do
    nil
  end

  def are_children_ready(n) do
    receive do
      {:ready} ->
        n = n - 1
        if n > 0 do
          are_children_ready(n)
        end
    end
  end

  def hear_children_gossip(n) do
    receive do
      {:received} ->
        n = n - 1
        IO.puts n
        if n > 0 do
          hear_children_gossip(n)
        end
    end
  end

  def hear_children_sum(n, threshold) do
    receive do
      {:converged, ratio} ->
        n = n - 1
        if n > threshold do
          hear_children_sum(n, threshold)
        else
          ratio
        end
    end
  end

  def create_workers(n, workers, num_nodes, topology, algorithm) do
    if n > 0 do
      workers = [
        %{
          id: n,
          start: {GS.Worker, :start_link,
            [%{
              :id => n,
              :count => 0,
              :parent => self(),
              :num_nodes => num_nodes,
              # :sum => n,
              # :weight => 1,
              :topology => topology,
              :algorithm => algorithm
            }]
          }
        }
        | workers]

      create_workers(n-1, workers, num_nodes, topology, algorithm)
    else
      workers
    end
  end

  def get_worker_ids(worker_ids, [child | children], idx) do
    {_, id, _, _} = child
    worker_ids = Map.put(worker_ids, idx, id)
    get_worker_ids(worker_ids, children, idx - 1)
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

      :push_sum -> push_sum(state, message)
    end
  end

  # private functions

  defp add_neighbors(state, message) do
    topology = Map.get(state, :topology)

    # TODO: Update sphere to whatever
    {neighbors, num_neighbors} =
      case topology do
        "full" -> get_full_neighbors(state, message)
        "3D" -> get_3D_neighbors(state, message)
        "rand2D" -> get_rand2D_neighbors(state, message)
        "sphere" -> get_sphere_neighbors(state, message)
        "line" -> get_line_neighbors(state, message)
        "imp2D" -> get_imp2D_neighbors(state, message)
      end

    new_state = Map.put(state, :neighbors, neighbors)
    new_state = Map.put(new_state, :num_neighbors, num_neighbors)
    super_pid = Map.get(new_state, :parent)
    Process.send(super_pid, {:ready}, [])
    {:noreply, new_state}
  end

  defp get_full_neighbors(state, message) do
    num_neighbors = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    last_pid = Map.get(message, num_neighbors)
    neighbors = Map.put(message, idx, last_pid)
    neighbors = Map.delete(neighbors, num_neighbors)
    {neighbors, num_neighbors - 1}
  end

  defp get_3D_neighbors(_state, _message) do

  end

  defp get_rand2D_neighbors(_state, _message) do

  end

  defp get_sphere_neighbors(_state, _message) do

  end

  defp get_line_neighbors(state, message) do
    idx = Map.get(state, :id)
    neighbors = %{}
    max_num = Map.get(state, :num_nodes)

    neighbors =
      cond do
        idx == 1 ->
          Map.put(neighbors, 1, Map.get(message, idx + 1))
        idx == max_num ->
          Map.put(neighbors, 1, Map.get(message, idx - 1))
        true ->
          neighbors = Map.put(neighbors, 1, Map.get(message, idx - 1))
          Map.put(neighbors, 2, Map.get(message, idx + 1))
      end

    num_neighbors =
      cond do
        idx == 1 -> 1
        idx == max_num -> 1
        true -> 2
      end

    {neighbors, num_neighbors}
  end

  defp get_imp2D_neighbors(state, message) do
    idx = Map.get(state, :id)
    neighbors = %{}
    max_num = Map.get(state, :num_nodes)

    neighbors =
      cond do
        idx == 1 ->
          neighbors = Map.put(neighbors, 1, Map.get(message, idx + 1))
          Map.put(neighbors, 2, Map.get(message, get_rand_num(max_num, idx)))
        idx == max_num ->
          neighbors = Map.put(neighbors, 1, Map.get(message, idx - 1))
          Map.put(neighbors, 2, Map.get(message, get_rand_num(max_num, idx)))
        true ->
          neighbors = Map.put(neighbors, 1, Map.get(message, idx - 1))
          neighbors = Map.put(neighbors, 2, Map.get(message, idx + 1))
          Map.put(neighbors, 3, Map.get(message, get_rand_num(max_num, idx)))
      end

    num_neighbors =
      cond do
        idx == 1 -> 2
        idx == max_num -> 2
        true -> 3
      end

    {neighbors, num_neighbors}

  end

  defp get_rand_num(max_num, idx) do
    rand = :rand.uniform(max_num)
    if rand == idx do
      get_rand_num(max_num, idx)
    else
      rand
    end
  end

  defp send_gossip(state, message) do
    count = Map.get(state, :count)
    if count < 10 do
      num_neighbors = Map.get(state, :num_neighbors)
      _self_idx = Map.get(state, :id)
      idx = :rand.uniform(num_neighbors)
      neighbor = get_in(state, [:neighbors, idx])

      Process.send(neighbor, {:receive_gossip, message}, [])

      #call yourself once in every 100 ms
      Process.send_after(self(), {:send_gossip, message}, 100)
    end

    {:noreply, state}
  end

  defp receive_gossip(state, message) do
    count = Map.get(state, :count)
    _idx = Map.get(state, :id)

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

  defp push_sum(state, message) do
    {received_sum, received_weight} = message
    count = Map.get(state, :count)
    _self_idx = Map.get(state, :id)
    # IO.inspect "#{self_idx}, #{count}"

    if count < 3 do
      {self_sum, self_weight} = {Map.get(state, :sum), Map.get(state, :weight)}
      {new_sum, new_weight} = {(received_sum + self_sum) / 2, (received_weight + self_weight) / 2}
      ratio_diff = abs((new_sum / new_weight) - (self_sum / self_weight))
      new_state = Map.put(state, :sum, new_sum)
      new_state = Map.put(new_state, :weight, new_weight)

      new_state =
        if ratio_diff < :math.pow(10, -10) do
          if count == 2 do
            parent_pid = Map.get(state, :parent)
            Process.send(parent_pid, {:converged, new_sum / new_weight}, [])
          end
          Map.update!(new_state, :count, &(&1 + 1))
        else
          Map.put(new_state, :count, 0)
        end

      num_neighbors = Map.get(new_state, :num_neighbors)
      idx = :rand.uniform(num_neighbors)
      neighbor = get_in(state, [:neighbors, idx])
      Process.send(neighbor, {:push_sum, {new_sum, new_weight}}, [])
      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end
end

GS.main()
