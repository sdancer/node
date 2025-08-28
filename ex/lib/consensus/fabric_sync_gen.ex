defmodule FabricSyncGen do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    :erlang.send_after(3000, self(), :tick)
    {:ok, state}
  end

  def handle_info(:tick, state) do
    cond do
      #true -> :erlang.send_after(300, self(), :tick)
      FabricGen.isSyncing() or FabricCoordinatorGen.isSyncing() or !FabricSyncAttestGen.hasQuorum() ->
        :erlang.send_after(30, self(), :tick)

      true ->
        {d1, d2, d3} = tick()
        if d1 < 100 or d2 < 100 do
          :erlang.send_after(600, self(), :tick)
        else
          :erlang.send_after(3000, self(), :tick)
        end
    end
    {:noreply, state}
  end

  def handle_info(:tick_oneshot, state) do
    tick()
    {:noreply, state}
  end

  def handle_call({:sync_entry, entry, attest, consensus}, _from, state) do
    {:reply, :ok, state}
  end



  def tick() do
    temporal = Consensus.chain_tip_entry()
    temporal_height = temporal.header_unpacked.height
    highest_temporal_height = FabricSyncAttestGen.highestTemporalHeight()
    upper_bound_temporal = min(temporal_height + 3000, highest_temporal_height)

    rooted = Fabric.rooted_tip_entry()
    rooted_height = rooted.header_unpacked.height

    rooted_delta = temporal_height - rooted_height

    if temporal_height < upper_bound_temporal and rooted_delta <= 6000 do
      next_temporal_holes = Enum.to_list(temporal_height..upper_bound_temporal)
      highest_peers_temporal = NodePeers.highest_height(%{min_temporal: upper_bound_temporal, sort: :temporal})
      len_holes_temp = length(next_temporal_holes)
      if len_holes_temp > 2 do
        #IO.puts "Syncing #{len_holes_temp} entries (#{inspect(hd(next_temporal_holes))})"
      end
      Enum.chunk_every(next_temporal_holes, 30)
      |> Enum.each(fn(chunk)->
        msg = NodeProto.catchup_tri(chunk)
        peer_ips = Enum.shuffle(highest_peers_temporal) |> Enum.map(& hd(&1)) |> Enum.take(1)
        send(NodeGen.get_socket_gen(), {:send_to_some, peer_ips, NodeProto.compress(msg)})
      end)
    end

    rooted = Fabric.rooted_tip_entry()
    rooted_height = rooted.header_unpacked.height
    highest_rooted_height = %{sort: :rooted}
    |> NodePeers.highest_height()
    |> List.first()
    |> case do
      nil -> rooted_height
      [_, _, _, highest | _ ] -> highest
    end
    upper_bound_rooted = min(temporal_height, min(rooted_height + 3000, highest_rooted_height))
    if rooted_height < upper_bound_rooted do
      next_rooted_holes = Enum.to_list(rooted_height..upper_bound_rooted)
      highest_peers_rooted = NodePeers.highest_height(%{min_rooted: upper_bound_rooted, sort: :rooted})
      len_holes_rooted = length(next_rooted_holes)
      if len_holes_rooted > 2 do
        IO.puts "Syncing #{len_holes_rooted} attestations"
      end
      #IO.inspect next_rooted_holes
      Enum.chunk_every(next_rooted_holes, 30)
      |> Enum.each(fn(chunk)->
        msg = NodeProto.catchup_bi(chunk)
        peer_ips = Enum.shuffle(highest_peers_rooted) |> Enum.map(& hd(&1)) |> Enum.take(1)
        send(NodeGen.get_socket_gen(), {:send_to_some, peer_ips, NodeProto.compress(msg)})
      end)
    end

    #roots are 0 but we are missing sigs
    if temporal_height - rooted_height > 0 do
    #if highest_rooted_height-rooted_height == 0 and temporal_height - rooted_height > 0 do
      next_holes = Enum.to_list(rooted_height..temporal_height)
      next_holes = Enum.take(next_holes, 29)
      pk_height = Enum.reduce(next_holes, %{}, fn(height, acc)->
        entry = Fabric.entries_by_height(height) |> Enum.random()
        trainers = Consensus.trainers_for_height(height)
        consensuses = Fabric.consensuses_by_entryhash(entry.hash)
        if !consensuses do
          Enum.reduce(trainers, acc, fn(pk, acc)->
            Map.put(acc, pk, Map.get(acc, pk, []) ++ [height])
          end)
        else
          {_, _score, c} = Consensus.best_by_weight(trainers, consensuses)
          trainers_signed = BLS12AggSig.unmask_trainers(trainers, c.mask)
          delta = trainers -- trainers_signed
          Enum.reduce(delta, acc, fn(pk, acc)->
            Map.put(acc, pk, Map.get(acc, pk, []) ++ [height])
          end)
        end
      end)
      Enum.group_by(pk_height, &elem(&1,1), &elem(&1,0))
      |> Enum.each(fn {heights, pks}->
        :erlang.spawn(fn()->
          msg = NodeProto.catchup_bi(heights) |> NodeProto.compress()
          peers = NodePeers.by_pks(pks) |> Enum.map(& &1.ip)
          send(NodeGen.get_socket_gen(), {:send_to_some, peers, msg})
        end)
      end)
    end

    #missing head entries
    tip_hashs = NodePeers.all_trainers()
    |> Enum.map(fn(p)->
      hash = get_in(p, [:temporal, :hash])
      height = get_in(p, [:temporal, :header_unpacked, :height])
      {p.pk, hash, height}
    end)
    |> Enum.filter(& elem(&1,1))
    |> Enum.reduce(%{}, fn({pk,hash,height},acc)-> Map.put(acc, {hash,height}, Map.get(acc,{hash,height},[]) ++ [pk]) end)

    #Only do it if there is more than 1 tip so some malicious node made a double entry
    if map_size(tip_hashs) > 1 do
      Enum.each(tip_hashs, fn {{hash,height}, pks} ->
        exists = Fabric.entry_by_hash(hash)
        if !exists do
          msg = NodeProto.catchup_tri([height]) |> NodeProto.compress()
          ips = NodePeers.by_pks(pks) |> Enum.map(& &1.ip)
          send(NodeGen.get_socket_gen(), {:send_to_some, ips, msg})
        end
      end)
    end

    {highest_temporal_height-temporal_height, highest_rooted_height-rooted_height, temporal_height - rooted_height}
  end
end
