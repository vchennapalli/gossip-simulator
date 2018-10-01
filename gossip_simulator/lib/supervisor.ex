defmodule GS do

  def main do
    [num_nodes, topology, algorithm] = System.argv()
    num_nodes = String.to_integer(num_nodes)
    GS.Supervisor.main(num_nodes, topology, algorithm)

  end

end

defmodule GS.Supervisor do
  @moduledoc """
  Documentation for GS.
  """
  use Supervisor

  def main(num_nodes, topology, algorithm) do

    topology =
      if topology == "rand2D" and num_nodes < 600 do
        IO.puts "No. of nodes is < 600. To avoid the case of non-convergence, modifying the topology to 2D grid"
        "2D"
      else
        topology
      end

    num_nodes =
      case topology do
        "3D" -> :math.pow(num_nodes, 1 / 3) |> :math.ceil() |> Kernel.trunc() |> :math.pow(3) |> Kernel.trunc()
        "torus" -> :math.pow(num_nodes, 1 / 2) |> :math.ceil() |> Kernel.trunc() |> :math.pow(2) |> Kernel.trunc()
        "2D" -> :math.pow(num_nodes, 1 / 2) |> :math.ceil() |> Kernel.trunc() |> :math.pow(2) |> Kernel.trunc()
        _ -> num_nodes
      end

    initialize(num_nodes, topology, algorithm)

    if algorithm == "gossip" do
      hear_children_gossip(num_nodes)
    else
      cpc = 0.0  # convergence percentage condition
      threshold = round(num_nodes * cpc)
      _ratio = hear_children_sum(num_nodes, threshold)
    end

  end

  def initialize(num_nodes, topology, algorithm) do
    start = num_nodes
    {:ok, super_pid} = start_link(num_nodes, topology, algorithm)
    worker_ids = get_worker_ids(%{}, Supervisor.which_children(super_pid), start, topology)
    send_neighbors(num_nodes, worker_ids, topology)
    are_children_ready(num_nodes)
    begin_gossip(num_nodes, worker_ids, topology, algorithm)
  end

  def begin_gossip(num_nodes, worker_ids, topology, algorithm) do
    start = :rand.uniform(num_nodes)

    {starter_pid, _, _} =
      if topology == "rand2D" do
        Map.get(worker_ids, start)
      else
        {Map.get(worker_ids, start), nil, nil}
      end

    # IO.inspect starter_pid
    if algorithm == "gossip" do
      Process.send(starter_pid, {:receive_gossip, "message"}, [])
    end
    # else
    #   Process.send(starter_pid, {:push_sum, {0, 0}}, [])
    # end

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
        # IO.puts n
        if n > 0 do
          hear_children_gossip(n)
        end
    end
  end

  def hear_children_sum(n, threshold) do
    receive do
      {:converged, ratio} ->
        IO.puts "#{ratio}, #{n}"
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
              :max_count => 3,
              :parent => self(),
              :num_nodes => num_nodes,
              :sum => n,
              :weight => 1,
              :ratio => n,
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

  def get_worker_ids(worker_ids, [child | children], idx, topology) do
    {_, id, _, _} = child
    id =
      if topology == "rand2D" do
        {id, :rand.uniform() |> Float.round(4), :rand.uniform() |> Float.round(4)}
      else
        id
      end
    worker_ids = Map.put(worker_ids, idx, id)
    get_worker_ids(worker_ids, children, idx - 1, topology)
  end

  def get_worker_ids(worker_ids, [], _, _) do
    worker_ids
  end

  def send_neighbors(n, worker_ids, topology) do
    if n > 0 do
      {worker_id, _, _} =
        if topology == "rand2D" do
          Map.get(worker_ids, n)
        else
          {Map.get(worker_ids, n), nil, nil}
        end

      Process.send(worker_id, {:add_neighbors, worker_ids}, [])
      send_neighbors(n-1, worker_ids, topology)
    end
  end
end

defmodule GS.Worker do
  use GenServer

  def start_link(status) do
    GenServer.start_link(__MODULE__, status)
  end

  def init(args) do
    algorithm = Map.get(args, :algorithm)
    if algorithm == "push-sum" do
      Process.send_after(self(), {:push_sum, nil}, 100)
    end
    {:ok, args}
  end

  # Callbacks

  def handle_info({tag, message}, state) do

    case tag do
      :add_neighbors -> add_neighbors(state, message)

      :receive_gossip -> receive_gossip(state, message)
      :send_gossip -> send_gossip(state, message)

      :push_sum -> push_sum(state, message)
      :pull_sum -> pull_sum(state, message)
    end
  end

  # private functions

  defp add_neighbors(state, all_nodes) do
    topology = Map.get(state, :topology)
    # idx = Map.get(state, :id)
    {neighbors, num_neighbors} =
      case topology do
        "full" -> get_full_neighbors(state, all_nodes)
        "2D" -> get_2D_neighbors(state, all_nodes)
        "3D" -> get_3D_neighbors(state, all_nodes)
        "rand2D" -> get_rand2D_neighbors(state, all_nodes)
        "torus" -> get_torus_neighbors(state, all_nodes)
        "line" -> get_line_neighbors(state, all_nodes)
        "imp2D" -> get_imp2D_neighbors(state, all_nodes)
      end
    # IO.inspect {idx, neighbors}
    new_state = Map.put(state, :neighbors, neighbors)
    new_state = Map.put(new_state, :num_neighbors, num_neighbors)
    super_pid = Map.get(new_state, :parent)
    Process.send(super_pid, {:ready}, [])
    {:noreply, new_state}
  end

  defp get_full_neighbors(state, all_nodes) do
    num_nodes = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    last_pid = Map.get(all_nodes, num_nodes)
    neighbors = Map.put(all_nodes, idx, last_pid)
    neighbors = Map.delete(neighbors, num_nodes)
    {neighbors, num_nodes - 1}
  end

  defp get_2D_neighbors(state, all_nodes) do
    num_nodes = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    side = :math.pow(num_nodes, 1/2) |> :math.ceil() |> Kernel.trunc()

    {y, x} = {div(idx, side), rem(idx, side)}
    {y, x} =
      if x == 0 do
        {y - 1, side}
      else
        {y, x}
      end

    num_neighbors = 1
    offsets = [{0, 1}, {0, -1}, {1, 0}, {-1, 0}]
    get_possible_2D_neighbors(all_nodes, {x, y}, offsets, %{}, side, num_neighbors)

  end

  defp get_possible_2D_neighbors(all_nodes, {x, y}, [{xo, yo} | offsets], neighbors, side, num_neighbors) do
    {xn, yn} = {x + xo, y + yo}
    {neighbors, num_neighbors} =
      if 1 <= xn and xn <= side and 0 <= yn and yn < side do
        neighbor_idx = xn + (yn * side)
        {Map.put(neighbors, num_neighbors, Map.get(all_nodes, neighbor_idx)), num_neighbors + 1}
      else
        {neighbors, num_neighbors}
      end

    get_possible_2D_neighbors(all_nodes, {x, y}, offsets, neighbors, side, num_neighbors)
  end

  defp get_possible_2D_neighbors(_, _, [], neighbors, _, num_neighbors) do
    {neighbors, num_neighbors - 1}
  end

  defp get_3D_neighbors(state, all_nodes) do
    num_nodes = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    side = :math.pow(num_nodes, 1/3) |> :math.ceil() |> Kernel.trunc()

    {z, yx} = {div(idx, side * side), rem(idx, side * side)}
    {z, yx} =
      if yx == 0 do
        {z - 1, side * side}
      else
        {z, yx}
      end

    {y, x} = {div(yx, side), rem(yx, side)}
    {y, x} =
      if x == 0 do
        {y - 1, side}
      else
        {y, x}
      end

    num_neighbors = 1
    offsets = [{1, 0, 0}, {-1, 0, 0}, {0, 1, 0}, {0, -1, 0}, {0, 0, 1}, {0, 0, -1}]
    get_possible_3D_neighbors(all_nodes, {x, y, z}, offsets, %{}, side, num_neighbors)
  end

  defp get_possible_3D_neighbors(all_nodes, {x, y, z}, [{xo, yo, zo} | offsets], neighbors, side, num_neighbors) do

    {xn, yn, zn} = {x + xo, y + yo, z + zo}
    {neighbors, num_neighbors} =
      if 1 <= xn and xn <= side and 0 <= yn and yn < side and 0 <= zn and zn < side do
        neighbor_idx = xn + (yn * side) + (zn * side * side)
        {Map.put(neighbors, num_neighbors, Map.get(all_nodes, neighbor_idx)), num_neighbors + 1}
      else
        {neighbors, num_neighbors}
      end
    get_possible_3D_neighbors(all_nodes, {x, y, z}, offsets, neighbors, side, num_neighbors)
  end

  defp get_possible_3D_neighbors(_, _, [], neighbors, _, num_neighbors) do
    {neighbors, num_neighbors - 1}
  end

  defp get_rand2D_neighbors(state, all_nodes) do
    num_nodes = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    {_, x, y} = Map.get(all_nodes, idx)
    last_tuple = Map.get(all_nodes, num_nodes)
    all_nodes = Map.put(all_nodes, idx, last_tuple)
    all_nodes = Map.delete(all_nodes, num_nodes)
    num_nodes = num_nodes - 1
    num_neighbors = 1
    get_possible_rand2D_neighbors(all_nodes, {x, y}, %{}, num_neighbors, num_nodes)
  end

  defp get_possible_rand2D_neighbors(all_nodes, {x, y}, neighbors, num_neighbors, idx) do

    if idx > 0 do
      {neighbor_pid, xo, yo} = Map.get(all_nodes, idx)
      distance = :math.sqrt(:math.pow(x - xo, 2) + :math.pow(y - yo, 2))
      {neighbors, num_neighbors} =
      if distance <= 1.0 do
        {Map.put(neighbors, num_neighbors, neighbor_pid), num_neighbors + 1}
      else
        {neighbors, num_neighbors}
      end
      get_possible_rand2D_neighbors(all_nodes, {x, y}, neighbors, num_neighbors, idx-1)
    else
      {neighbors, num_neighbors - 1}
    end
  end

  defp get_torus_neighbors(state, all_nodes) do
    num_nodes = Map.get(state, :num_nodes)
    idx = Map.get(state, :id)
    side = :math.pow(num_nodes, 1/2) |> :math.ceil() |> Kernel.trunc()

    {y, x} = {div(idx, side), rem(idx, side)}
    {y, x} =
      if x == 0 do
        {y - 1, side}
      else
        {y, x}
      end

    num_neighbors = 1
    offsets = [{0, 1}, {0, -1}, {1, 0}, {-1, 0}]
    get_possible_torus_neighbors(all_nodes, {x, y}, offsets, %{}, side, num_neighbors)
  end


  defp get_possible_torus_neighbors(all_nodes, {x, y}, [{xo, yo} | offsets], neighbors, side, num_neighbors) do
    {xn, yn} = {x + xo, y + yo}

    xn =
      cond do
        xn == 0 -> side
        xn == side + 1 -> 1
        true -> xn
      end

    yn =
      cond do
        yn == -1 -> side - 1
        yn == side -> 0
        true -> yn
      end

    neighbor_idx = xn + (yn * side)
    neighbors = Map.put(neighbors, num_neighbors, Map.get(all_nodes, neighbor_idx))

    get_possible_torus_neighbors(all_nodes, {x, y}, offsets, neighbors, side, num_neighbors + 1)
  end

  defp get_possible_torus_neighbors(_, _, [], neighbors, _, num_neighbors) do
    {neighbors, num_neighbors - 1}
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

  defp push_sum(state, _) do
    count = Map.get(state, :count)
    max_count = Map.get(state, :max_count)
    if count < max_count do
      new_state = Map.update!(state, :sum, &(&1/2))
      new_state = Map.update!(new_state, :weight, &(&1/2))

      {self_sum, self_weight} = {Map.get(new_state, :sum), Map.get(new_state, :weight)}
      present_ratio = self_sum / self_weight
      previous_ratio = Map.get(new_state, :ratio)

      new_state =
        if abs(present_ratio - previous_ratio) < :math.pow(10, -10) do

          if count == max_count - 1 do
            parent_pid = Map.get(new_state, :parent)
            Process.send(parent_pid, {:converged, present_ratio}, [])
          end

          Map.update!(new_state, :count, &(&1 + 1))
        else
          Map.put(new_state, :count, 0)
        end

      new_state = Map.put(new_state, :ratio, present_ratio)
      num_neighbors = Map.get(new_state, :num_neighbors)
      idx = :rand.uniform(num_neighbors)
      neighbor = get_in(state, [:neighbors, idx])
      Process.send(neighbor, {:pull_sum, {self_sum, self_weight}}, [])
      Process.send_after(self(), {:push_sum, nil}, 100)

      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end

  defp pull_sum(state, {received_sum, received_weight}) do
    ratio = Map.get(state, :ratio)
    count = Map.get(state, :count)
    # IO.inspect self()
    # IO.puts "HERE, #{ratio}, #{count}"
    count = Map.get(state, :count)
    max_count = Map.get(state, :max_count)
    new_state =
      if count < max_count do
        new_state = Map.update!(state, :sum, &(&1 + received_sum))
        Map.update!(new_state, :weight, &(&1 + received_weight))
      else
        state
      end

    {:noreply, new_state}
  end


end

GS.main()
