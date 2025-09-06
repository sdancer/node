defmodule BIC.Epoch.Difficulty do
  import ConsensusKV

  @diff_default_bits 24
  @sols_hi 256_000
  @sols_lo 16_000
  @diff_min_bits 24
  @diff_max_bits 64

  def difficulty_bits(epoch) do
    db = kv_get("bic:epoch:difficulty_bits:#{epoch}", %{to_integer: true}) || @diff_default_bits
    max(@diff_min_bits, db)
  end

  defp clamp_bits(b), do: b |> max(@diff_min_bits) |> min(@diff_max_bits)

  defp ceil_log2(x) when x <= 1.0, do: 0

  defp ceil_log2(x) do
    (:math.log(x) / :math.log(2.0)) |> Float.ceil() |> trunc()
  end

  defp next_bits_from(prev_bits, sols) when is_integer(sols) and sols >= 0 do
    cond do
      sols > @sols_hi ->
        delta = ceil_log2(sols / @sols_hi)
        clamp_bits(prev_bits + max(1, delta))

      sols == 0 ->
        clamp_bits(prev_bits - 1)

      sols < @sols_lo ->
        delta = ceil_log2(@sols_lo / sols)
        clamp_bits(prev_bits - max(1, delta))

      true ->
        prev_bits
    end
  end
end
