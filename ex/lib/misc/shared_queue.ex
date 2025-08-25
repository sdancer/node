defmodule SharedQueue do
  use GenServer

  ## --- client API ---

  def start_link(range \\ 0..281) do
    GenServer.start_link(__MODULE__, range, name: __MODULE__)
  end

  # drains n items, returns them to caller
  def drain(n) do
    GenServer.call(__MODULE__, {:drain, n})
  end

  # drains everything
  def drain_all do
    GenServer.call(__MODULE__, :drain_all)
  end

  ## --- server callbacks ---

  def init(range) do
    q = :queue.from_list(Enum.to_list(range))
    {:ok, q}
  end

  def handle_call({:drain, n}, _from, q) do
    {taken, rest} = take_n(q, n, [])
    {:reply, Enum.reverse(taken), rest}
  end

  def handle_call(:drain_all, _from, q) do
    list = :queue.to_list(q)
    {:reply, list, :queue.new()}
  end

  ## --- helpers ---

  defp take_n(q, 0, acc), do: {acc, q}

  defp take_n(q, n, acc) do
    case :queue.out(q) do
      {{:value, x}, q2} -> take_n(q2, n - 1, [x | acc])
      {:empty, _} -> {acc, q}
    end
  end
end

