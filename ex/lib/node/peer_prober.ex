defmodule Node.PeerProber do
  use GenServer

  @tab :TEMP_PEERS

  # Rate limiting
  @tick_ms Application.compile_env(:ama, :temp_probe_tick_ms, 2_000) # every 2s
  @burst   Application.compile_env(:ama, :temp_probe_burst, 2)
  @rate    Application.compile_env(:ama, :temp_probe_rate, 1)  # tokens added per tick

  @connect_timeout Application.compile_env(:ama, :temp_probe_connect_timeout_ms, 800)
  @handshake_timeout Application.compile_env(:ama, :temp_probe_handshake_timeout_ms, 600)

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def init(state) do
    # Ensure tables exist
    NodePeers.ensure_tables!()

    # token bucket
    state = Map.merge(state, %{tokens: @burst})
    Process.send_after(self(), :tick, @tick_ms)
    {:ok, state}
  end

  def handle_info(:tick, %{tokens: tokens} = state) do
    tokens = min(tokens + @rate, @burst)

    # choose how many we can probe now
    capacity = tokens
    batch = if capacity > 0, do: NodePeers.temp_due_batch(capacity), else: []

    new_tokens =
      Enum.reduce(batch, tokens, fn entry, acc ->
        # fire-and-forget each probe
        Task.start(fn -> probe_one(entry) end)
        acc - 1
      end)

    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, %{state | tokens: max(new_tokens, 0)}}
  end

  # ----------------------------------------------------
  # Probe logic
  # ----------------------------------------------------
  defp probe_one(entry = {{ip, port}, _rec}) do
    case tcp_probe(ip, port) do
      :ok    -> NodePeers.temp_on_success(entry)
      :error -> NodePeers.temp_on_failure(entry)
    end
  end

  defp tcp_probe(ip, port) do
    # Attempt TCP connect with a small timeout, then cleanly close.
    # Optionally add a tiny handshake or version byte to be extra sure.
    ip_char = String.to_charlist(ip)

    case :gen_tcp.connect(ip_char, port, [:binary, active: false, nodelay: true], @connect_timeout) do
      {:ok, sock} ->
        # Optional: write a 1-byte "ping" and expect a 1-byte "P" reply
        _ = :gen_tcp.send(sock, <<0x01>>)
        res =
          receive do
            {:tcp, ^sock, <<?P>>} -> :ok
            {:tcp_closed, ^sock}  -> :ok   # if your nodes close immediately, still treat as reachable
            _other                -> :ok
          after
            @handshake_timeout -> :ok       # don't be too strict; reaching is enough
          end

        :gen_tcp.close(sock)
        res

      {:error, _} ->
        :error
    end
  end
end
