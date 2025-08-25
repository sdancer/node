defmodule Node.PeersTemp do
  @temp_tab :TEMP_PEERS
  @node_tab :NODEPeers

  @promotion_confirms Application.compile_env(:ama, :temp_peer_promotion_confirms, 2)
  @max_failures        Application.compile_env(:ama, :temp_peer_max_failures, 6)
  @min_backoff_ms      Application.compile_env(:ama, :temp_peer_min_backoff_ms, 15_000)   # 15s
  @max_backoff_ms      Application.compile_env(:ama, :temp_peer_max_backoff_ms, 30 * 60_000) # 30m
  @expiry_ms           Application.compile_env(:ama, :temp_peer_expiry_ms, 7 * 24 * 60_60_000) # 7d

  # Call this once at app boot (e.g., in your application start/1)
  def ensure_tables!() do
    try do
      :ets.new(@temp_tab, [
        :set, :public, :named_table,
        {:read_concurrency, true}, {:write_concurrency, true}
      ]) 
    rescue :already_exists ->
     nil
    end

    # your NODEPeers likely already exists elsewhere
    :ok
  end

  @doc """
  Seed a temp peer (ip, port). Accepts binaries like "1.2.3.4" and integers.
  """
  def add_temp_peer(ip, port, source \\ :manual) do
    {ip_str, port_int} = {to_string(ip), port}
    key = {ip_str, port_int}
    now = System.system_time(:millisecond)
    rec = %{
      ip: ip_str,
      port: port_int,
      first_seen: now,
      last_attempt: nil,
      next_attempt: now + :rand.uniform(10_000), # 0–10s jittered initial wait
      attempts: 0,
      successes: 0,
      failures: 0,
      backoff_ms: @min_backoff_ms,
      source: source
    }

    :ets.insert(@temp_tab, {key, rec})
    :ok
  end

  @doc """
  Bulk add (useful after scraping a list).
  """
  def add_temp_peers(enumerable, source \\ :list) do
    Enum.each(enumerable, fn
      {ip, port} -> add_temp_peer(ip, port, source)
      ip when is_binary(ip) -> add_temp_peer(ip, 8080, source)          # default port if you want
      ip when is_tuple(ip)  -> add_temp_peer(:inet.ntoa(ip), 8080, source)
    end)
  end

  @doc """
  Promotion: move a confirmed temp peer into NODEPeers.
  """
  def promote_temp_peer!(ip, port, meta \\ %{}) do
    :ets.insert(@node_tab, {{ip, port}, Map.merge(%{ip: ip, port: port, static: false}, meta)})
    :ok
  end

  @doc """
  Delete temp peer (e.g., after promotion or expiry).
  """
  def drop_temp_peer(ip, port) do
    :ets.delete(@temp_tab, {ip, port})
    :ok
  end

  # ----------------------------------------------------
  # Internal utilities used by the prober
  # ----------------------------------------------------
  def temp_due_batch(limit) do
    now = System.system_time(:millisecond)

    :ets.tab2list(@temp_tab)
    |> Enum.filter(fn {{_ip, _port}, rec} ->
      rec.next_attempt <= now and not expired?(rec, now)
    end)
    |> Enum.shuffle()
    |> Enum.take(limit)
  end

  defp expired?(rec, now), do: (now - rec.first_seen) > @expiry_ms

  def temp_on_success({key = {ip, port}, rec}) do
    now = System.system_time(:millisecond)
    successes = rec.successes + 1

    # halve backoff on success; keep within bounds
    backoff = max(div(rec.backoff_ms, 2), @min_backoff_ms)
    next    = now + jitter(backoff)

    new = %{rec |
      last_attempt: now,
      attempts: rec.attempts + 1,
      successes: successes,
      next_attempt: next,
      backoff_ms: backoff
    }

    :ets.insert(@temp_tab, {key, new})

    if successes >= @promotion_confirms do
      # Move to main peers and drop from temp
      promote_temp_peer!(ip, port)
      :ets.delete(@temp_tab, key)
      {:promoted, {ip, port}}
    else
      :ok
    end
  end

  def temp_on_failure({key, rec}) do
    now = System.system_time(:millisecond)
    failures = rec.failures + 1

    # exponential backoff with cap
    backoff = min(max(rec.backoff_ms * 2, @min_backoff_ms), @max_backoff_ms)
    next    = now + jitter(backoff)

    new = %{rec |
      last_attempt: now,
      attempts: rec.attempts + 1,
      failures: failures,
      next_attempt: next,
      backoff_ms: backoff
    }

    :ets.insert(@temp_tab, {key, new})

    if failures >= @max_failures do
      :ets.delete(@temp_tab, key)
      :dropped
    else
      :ok
    end
  end

  defp jitter(base_ms) do
    # +/- 20% jitter
    delta = Float.round(base_ms * 0.2)
    base_ms + :rand.uniform(trunc(2 * delta + 1)) - trunc(delta)
  end
end
