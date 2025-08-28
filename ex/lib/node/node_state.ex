defmodule NodeState do
  import NodeProto

  def init() do
    %{
      challenges: %{},
    }
  end

  #TODO: anti local compute dos here
  def handle(:new_phone_who_dis, istate, term) do
    anr = NodeANR.verify_and_unpack(term.anr)
    if !!anr and is_integer(term.challenge) and istate.peer.ip == anr.ip4 do
      sk = Application.fetch_env!(:ama, :trainer_sk)
      pk = Application.fetch_env!(:ama, :trainer_pk)
      sig = BlsEx.sign!(sk, <<pk::binary, :erlang.integer_to_binary(term.challenge)::binary>>, BLS12AggSig.dst_anr_challenge())
      send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(NodeProto.what?(term.challenge, sig))})
      send(NodeGen, {:handle_sync, :new_phone_who_dis_ns, istate, %{anr: anr}})
    end
  end
  def handle(:new_phone_who_dis_ns, istate, term) do
    NodeANR.insert(term.anr)
    istate.ns
  end

  def handle(:what?, istate, term) do
    anr = NodeANR.verify_and_unpack(term.anr)

    #signed within 6 seconds
    ts = :os.system_time(1)
    delta = abs(ts - term.challenge)

    if !!anr and istate.peer.ip == anr.ip4 and delta <= 6 do
      challenge_bin = :erlang.integer_to_binary(term.challenge)
      if BlsEx.verify?(anr.pk, term.signature, <<anr.pk::binary, challenge_bin::binary>>, BLS12AggSig.dst_anr_challenge()) do
        send(NodeGen, {:handle_sync, :'what?_ns', istate, %{pk: anr.pk, anr: anr}})
      end
    end
  end
  def handle(:'what?_ns', istate, term) do
    NodeANR.insert(term.anr)
    NodeANR.set_handshaked(term.anr.pk)
    istate.ns
  end

  def handle(:ping, istate, term) do
    temporal = Entry.unpack(term.temporal)
    rooted = Entry.unpack(term.rooted)
    try do
      %{error: :ok, hash: hasht} = Entry.validate_signature(temporal.header, temporal.signature, temporal.header_unpacked.signer, temporal[:mask])
      %{error: :ok, hash: hashr} = Entry.validate_signature(rooted.header, rooted.signature, rooted.header_unpacked.signer, rooted[:mask])

      hasPermissionSlip = NodeANR.handshaked_and_valid_ip4(istate.peer.signer, istate.peer.ip)
      :erlang.spawn(fn()->
        if hasPermissionSlip do
          anrs = NodeANR.get_random_verified(3)
          if anrs != [], do: send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(NodeProto.peers_v2(anrs))})
        end
      end)

      term = %{temporal: Map.put(temporal, :hash, hasht), rooted: Map.put(rooted, :hash, hashr), ts_m: term.ts_m, hasPermissionSlip: hasPermissionSlip}
      send(NodeGen, {:handle_sync, :ping_ns, istate, term})
    catch
      e,{:badmatch, %{error: :wrong_epoch}} -> nil
      e,r -> #IO.inspect {:error_ping, e, r, term, istate.peer.ip}
      nil
    end
  end
  def handle(:ping_ns, istate, term) do
    peer_ip = istate.peer.ip

    if term.hasPermissionSlip or (istate.peer.signer in Consensus.trainers_for_height(Consensus.chain_height())) do
      peer = :ets.lookup_element(NODEPeers, peer_ip, 2, %{})
      peer = Map.merge(peer, %{
          ip: peer_ip,
          pk: istate.peer.signer, version: istate.peer.version,
          last_ping: :os.system_time(1000),
          last_msg: :os.system_time(1000),
          temporal: term.temporal, rooted: term.rooted,
      })

      peer = if !!peer[:shared_secret] do peer else
        shared_key = BlsEx.get_shared_secret!(istate.peer.signer, Application.fetch_env!(:ama, :trainer_sk))
        Map.put(peer, :shared_secret, shared_key)
      end

      :ets.insert(NODEPeers, {peer_ip, peer})

      :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [peer_ip], compress(NodeProto.pong(term.ts_m))}) end)
    end

    istate.ns
  end


  def handle(:pong, istate, term) do
    term = %{seen_time: :os.system_time(1000), ts_m: term.ts_m}
    send(NodeGen, {:handle_sync, :pong_ns, istate, term})
  end
  def handle(:pong_ns, istate, term) do
    peer_ip = istate.peer.ip
    peer = :ets.lookup_element(NODEPeers, peer_ip, 2, %{})
    latency = term.seen_time - term.ts_m
    peer = Map.merge(peer, %{latency: latency,
      ip: peer_ip, pk: istate.peer.signer,
      last_pong: term.seen_time, last_msg: :os.system_time(1000)})
    :ets.insert(NODEPeers, {peer_ip, peer})

    istate.ns
  end

  def handle(:txpool, istate, term) do
    good = Enum.filter(term.txs_packed, & TX.validate(&1).error == :ok)
    TXPool.insert(good)
  end

  def handle(:peers_v2, istate, term) do
    anrs = Enum.map(term.anrs, & NodeANR.verify_and_unpack(&1))
    |> Enum.filter(& &1)
    term = %{anrs: anrs}
    send(NodeGen, {:handle_sync, :peers_v2_ns, istate, term})
  end
  def handle(:peers_v2_ns, istate, term) do
    Enum.each(term.anrs, fn(anr)->
      NodeANR.insert(anr)
    end)
    istate.ns
  end

  #def handle(:sol, istate, term) do nil end
  def handle(:sol, istate, term) do
    sol = BIC.Sol.unpack(term.sol)
    trainer_pk = Application.fetch_env!(:ama, :trainer_pk)
    cond do
      sol.epoch != Consensus.chain_epoch() ->
        #IO.inspect {:broadcasted_sol_invalid_epoch, sol.epoch, Consensus.chain_epoch()}
        nil
      !BIC.Sol.verify(term.sol, %{vr_b3: :crypto.strong_rand_bytes(32)}) ->
        IO.inspect {:peer_sent_invalid_sol, :TODO_block_malicious_peer}
        nil
      !BlsEx.verify?(sol.pk, sol.pop, sol.pk, BLS12AggSig.dst_pop()) ->
        IO.inspect {:peer_sent_invalid_sol_pop, :TODO_block_malicious_peer}
        nil
      !TXPool.add_gifted_sol(term.sol) ->
        IO.inspect {:peer_sent_duplicate_sol, :TODO_block_malicious_peer}
        nil
      trainer_pk == sol.pk and Consensus.chain_balance(trainer_pk) >= BIC.Coin.to_flat(1) ->
        sk = Application.fetch_env!(:ama, :trainer_sk)
        #self compute
        if trainer_pk == sol.computor do
          tx_packed1 = TX.build(sk, "Epoch", "submit_sol", [term.sol])
          TXPool.insert([tx_packed1])
          NodeGen.broadcast(:txpool, :trainers, [[tx_packed1]])
        else
          #IO.inspect {:peer_sent_sol, Base58.encode(istate.peer.signer)}
          tx_packed1 = TX.build(sk, "Epoch", "submit_sol", [term.sol])
          tx_packed2 = TX.build(sk, "Coin", "transfer", [sol.computor, :erlang.integer_to_binary(BIC.Coin.to_cents(100)), "AMA"])
          TXPool.insert([tx_packed1, tx_packed2])
          NodeGen.broadcast(:txpool, :trainers, [[tx_packed1, tx_packed2]])
        end
      true -> nil
    end
  end

  def handle(:entry, istate, term) do
    seen_time = :os.system_time(1000)

    #atom hash: 32 bytes hash
    pattern = :binary.compile_pattern([<<119, 4, 104, 97, 115, 104, 109, 0, 0, 0, 32>>])
    {exists?, hash} = case :binary.match(term.entry_packed, pattern) do
      {pos, len} ->
        start_pos = pos + len
        hash = binary_part(term.entry_packed, start_pos, 32)
        %{db: db} = :persistent_term.get({:rocksdb, Fabric})
        {!!RocksDB.get(hash, %{db: db}), hash}
      :nomatch ->
        IO.inspect {:invalid_entry?}
        {nil, nil}
    end

    if !exists? do
      %{error: :ok, entry: entry} = Entry.unpack_and_validate(term.entry_packed)
      if Entry.height(entry) >= Fabric.rooted_tip_height() do
        #IO.inspect {:insert, Base58.encode(entry.hash), :os.system_time(1000)}
        case Fabric.insert_entry(entry, seen_time) do
          :ok ->
            #trainer_for_slot = Consensus.trainer_for_slot(Entry.height(entry), entry.header_unpacked.slot+1)
            #if trainer_for_slot == Application.fetch_env!(:ama, :trainer_pk) do
            #end
            send(FabricGen, :tick_oneshot)
          {:error, {:error, ~c"Resource busy: "}} -> :ok
            #IO.inspect {:insert_entry, :resource_busy, Base58.encode(entry.hash)}
        end
      end
    else
      #IO.inspect {:already_exists, Base58.encode(hash)}
    end

    cond do
        !!term[:consensus_packed] ->
            c = Consensus.unpack(term.consensus_packed)
            send(FabricCoordinatorGen, {:validate_consensus, c})

        !!term[:attestation_packed] ->
            %{error: :ok, attestation: a} = Attestation.unpack_and_validate(term.attestation_packed)
            send(FabricCoordinatorGen, {:add_attestation, a})

        true -> :ok
    end
  end

  def handle(:attestation_bulk, istate, term) do
    #IO.inspect {:got, :attestation_bulk,  istate.peer.ip, term}
    Enum.each(term.attestations_packed, fn(attestation_packed)->
        res = Attestation.unpack_and_validate(attestation_packed)
        if res.error == :ok and Attestation.validate_vs_chain(res.attestation) do
          send(FabricCoordinatorGen, {:add_attestation, res.attestation})
          #IO.inspect {:attest, res.attestation.entry_hash |> Base58.encode(), :os.system_time(1000)}
        else
          :ets.insert(AttestationCache, {{res.attestation.entry_hash, res.attestation.signer}, {res.attestation, :os.system_time(1000)}})
          #IO.inspect {:late_attest, res.attestation.entry_hash |> Base58.encode(), :os.system_time(1000)}
        end
    end)
  end

  def handle(:consensus_bulk, istate, term) do
    Enum.each(term.consensuses_packed, fn(consensus_packed)->
        c = Consensus.unpack(consensus_packed)
        send(FabricCoordinatorGen, {:validate_consensus, c})
    end)
  end

  def handle(:catchup_entry, istate, term) do
    true = length(term.heights) <= 100
    Enum.each(term.heights, fn(height)->
        case Fabric.entries_by_height(height) do
            [] -> nil
            map_entries ->
              Enum.each(map_entries, fn(map)->
                msg = NodeProto.entry(%{entry_packed: Entry.pack(map.entry)})
                :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
              end)
        end
    end)
  end

  def handle(:catchup_tri, istate, term) do
    true = length(term.heights) <= 30

    Enum.each(term.heights, fn(height)->
        case Fabric.get_entries_by_height_w_attestation_or_consensus(height) do
            [] -> nil
            map_entries ->
              Enum.each(map_entries, fn(map)->
                msg = cond do
                  map[:consensus] ->
                    NodeProto.entry(%{entry_packed: Entry.pack(map.entry), consensus_packed: Consensus.pack(map.consensus)})
                  map[:attest] ->
                    NodeProto.entry(%{entry_packed: Entry.pack(map.entry), attestation_packed: Attestation.pack(map.attest)})
                  true ->
                    NodeProto.entry(%{entry_packed: Entry.pack(map.entry)})
                end
                :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
              end)
        end
    end)
  end

  def handle(:catchup_bi, istate, term) do
    true = length(term.heights) <= 30

    {attestations_packed, consensuses_packed} = Enum.reduce(term.heights, {[], []}, fn(height, {a, c})->
        {attests, consens} = Fabric.get_attestations_and_consensuses_by_height(height)
        attests = Enum.map(attests, & Attestation.pack(&1))
        consens = Enum.map(consens, & Consensus.pack(&1))
        {a ++ attests, c ++ consens}
    end)

    if length(attestations_packed) > 0 do
        msg = NodeProto.attestation_bulk(attestations_packed)
        :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
    end

    if length(consensuses_packed) > 0 do
        msg = NodeProto.consensus_bulk(consensuses_packed)
        :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
    end
  end

  def handle(:catchup_attestation, istate, term) do
    #IO.inspect {:got, :catchup_attestation,  istate.peer.ip}

    true = length(term.hashes) <= 30

    attestations_packed = Enum.map(term.hashes, fn(hash)->
      Fabric.my_attestation_by_entryhash(hash)
    end)
    |> Enum.filter(& &1)
    |> Enum.map(& Attestation.pack(&1))

    if length(attestations_packed) > 0 do
        msg = NodeProto.attestation_bulk(attestations_packed)
        :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
    end
  end

  def handle(:special_business, istate, term) do
    op = term.business.op
    cond do
      #istate.peer.pk != <<>> -> nil
      op == "slash_trainer_tx" ->
        signature = SpecialMeetingAttestGen.maybe_attest("slash_trainer_tx", term.business.epoch, term.business.malicious_pk)
        if signature do
          pk = Application.fetch_env!(:ama, :trainer_pk)
          business = %{op: "slash_trainer_tx_reply", epoch: term.business.epoch, malicious_pk: term.business.malicious_pk,
            pk: pk, signature: signature}
          msg = NodeProto.special_business_reply(business)
          :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
        end
      op == "slash_trainer_entry" ->
        signature = SpecialMeetingAttestGen.maybe_attest("slash_trainer_entry", term.business.entry_packed)
        entry = Entry.unpack(term.business.entry_packed)
        if signature do
          pk = Application.fetch_env!(:ama, :trainer_pk)
          business = %{op: "slash_trainer_entry_reply", entry_hash: entry.hash, pk: pk, signature: signature}
          msg = NodeProto.special_business_reply(business)
          :erlang.spawn(fn()-> send(NodeGen.get_socket_gen(), {:send_to_some, [istate.peer.ip], compress(msg)}) end)
        end
    end
  end

  def handle(:special_business_reply, istate, term) do
    #IO.inspect {:special_business_reply, term.business}
    op = term.business.op
    cond do
      #istate.peer.pk != <<>> -> nil
      op == "slash_trainer_tx_reply" ->
        b = term.business
        msg = <<"slash_trainer", b.epoch::32-little, b.malicious_pk::binary>>
        sigValid = BlsEx.verify?(b.pk, b.signature, msg, BLS12AggSig.dst_motion())
        if sigValid do
          send(SpecialMeetingGen, {:add_slash_trainer_tx_reply, term.business.pk, term.business.signature})
        end

      op == "slash_trainer_entry_reply" ->
        b = term.business
        sigValid = BlsEx.verify?(b.pk, b.signature, b.entry_hash, BLS12AggSig.dst_entry())
        if sigValid do
          send(SpecialMeetingGen, {:add_slash_trainer_entry_reply, b.entry_hash, b.pk, b.signature})
        end
    end
  end

  def handle(:solicit_entry, istate, term) do
    hash = term.hash
    hash_b58 = Base58.encode(hash)
    entry = Fabric.entry_by_hash(hash)
    entry_height = Entry.height(entry)
    %{entries: entries} = API.Chain.by_height(entry_height)
    highest_score = Enum.sort_by(entries, & &1[:consensus][:score] || 0, :desc)
    |> List.first()
    |> case do
      %{consensus: %{score: score}} -> score
      _ -> 0
    end

    cur_hash = Consensus.chain_tip()
    cur_hash_b58 = Base58.encode(cur_hash)
    cur_score = Enum.find_value(entries, 0, & &1.hash == cur_hash_b58 && &1.consensus.score)

    trainers = Consensus.trainers_for_height(entry_height)

    cond do
      istate.peer.signer not in trainers -> nil
      entry_height <= Fabric.rooted_tip_height() -> nil
      length(entries) < 2 -> nil
      #cur_score >= highest_score -> nil
      true -> FabricSnapshot.backstep_temporal([cur_hash])
    end
  end

  def handle(:solicit_entry2, istate, term) do
    %{hash: cur_hash, header_unpacked: %{height: cur_height}} = Consensus.chain_tip_entry()
    trainers = Consensus.trainers_for_height(cur_height+1)
    if istate.peer.signer in trainers do
      [{best_entry, _mut_hash, _score}|_] = FabricGen.best_entry_for_height_no_score(cur_height)
      if cur_hash != best_entry.hash do
        Consensus.chain_rewind(cur_hash)
      end
    end
  end

  def handle(_, _, _) do
  end
end
