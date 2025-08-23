defmodule NodeGen do
  use GenServer

  def start_link(ip_tuple, port) do
    GenServer.start_link(__MODULE__, [ip_tuple, port], name: __MODULE__)
  end

  def init([ip_tuple, _port]) do
    ip = Tuple.to_list(ip_tuple) |> Enum.join(".")
    NodeANR.seed()
    NodePeers.seed(ip)

    state = %{
      ns: NodeState.init()
    }

    :erlang.send_after(1000, self(), :tick)
    :erlang.send_after(1000, self(), :tick_ping)
    :erlang.send_after(1000, self(), :tick_anr)
    {:ok, state}
  end

  def get_socket_gen() do
    idx = :rand.uniform(8) - 1
    :'NodeGenSocketGen#{idx}'
  end

  def get_reassembly_gen(pk, ts_nano) do
    idx = :erlang.phash2({pk, ts_nano}, 32)
    :'NodeGenReassemblyGen#{idx}'
  end

  def broadcast_ping() do
    :erlang.spawn(fn()->
      msg = NodeProto.ping()
      ips = NodePeers.all() |> Enum.map(& &1.ip)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def broadcast_check_anr() do
    my_pk = Application.fetch_env!(:ama, :trainer_pk)
    NodeANR.get_random_unverified(1)
    |> Enum.filter(& elem(&1,0) != my_pk)
    |> Enum.each(fn({pk, ip})->
      IO.inspect {:anr_request_to, ip}
      challenge = :os.system_time(1)
      :erlang.spawn(fn()->
        msg = NodeProto.new_phone_who_dis(challenge)
        send(get_socket_gen(), {:send_to_some, [ip], NodeProto.compress(msg)})
      end)
    end)
  end

  def broadcast(:txpool, who, [txs_packed]) do
    :erlang.spawn(fn()->
      msg = NodeProto.txpool(txs_packed)
      ips = NodePeers.by_who(who)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def broadcast(:entry, who, [map]) do
    :erlang.spawn(fn()->
      msg = NodeProto.entry(map)
      ips = NodePeers.by_who(who)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def broadcast(:attestation_bulk, who, [attestations_packed]) do
    :erlang.spawn(fn()->
      msg = NodeProto.attestation_bulk(attestations_packed)
      ips = NodePeers.by_who(who)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def broadcast(:sol, who, [sol]) do
    :erlang.spawn(fn()->
      msg = NodeProto.sol(sol)
      ips = NodePeers.by_who(who)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def broadcast(:special_business, who, [business]) do
    :erlang.spawn(fn()->
      msg = NodeProto.special_business(business)
      ips = NodePeers.by_who(who)
      send(get_socket_gen(), {:send_to_some, ips, NodeProto.compress(msg)})
    end)
  end

  def tick() do
    NodePeers.clear_stale()
  end

  def handle_info(msg, state) do
    state = case msg do

      :tick ->
        :erlang.send_after(1000, self(), :tick)
        tick()
        state

      :tick_ping ->
        :erlang.send_after(500, self(), :tick_ping)
        broadcast_ping()
        state

      :tick_anr ->
        :erlang.send_after(3000, self(), :tick_anr)
        broadcast_check_anr()
        state

      {:handle_sync, op, innerstate, args} ->
        #TODO: ns dropped
        innerstate = Map.put(innerstate, :ns, state.ns)
        innerstate = NodeState.handle(op, innerstate, args)
        Map.put(state, :ns, innerstate)

    end
    {:noreply, state}
  end
end
