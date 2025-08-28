defmodule API.TX do
    def get_tip(num) do
        tx_id = if byte_size(tx_id) != 32, do: Base58.decode(tx_id), else: tx_id
        Consensus.chain_tx(tx_id)
        |> format_tx_for_client()
    end

    def get(tx_id) do
        tx_id = if byte_size(tx_id) != 32, do: Base58.decode(tx_id), else: tx_id
        Consensus.chain_tx(tx_id)
        |> format_tx_for_client()
    end

    def get_by_entry(entry_hash) do
        entry_hash = if byte_size(entry_hash) != 32, do: Base58.decode(entry_hash), else: entry_hash
        case Fabric.entry_by_hash(entry_hash) do
            nil -> nil
            %{hash: entry_hash, header_unpacked: %{slot: slot}, txs: txs} ->
                Enum.map(txs, fn(tx_packed)->
                    txu = TX.unpack(tx_packed)
                    |> Map.put(:metadata, %{entry_hash: entry_hash, entry_slot: slot})
                    format_tx_for_client(txu)
                end)
        end
    end

    def get_by_address(pk, filters) do
        {_, txs_sent} = get_by_address_sent(pk, filters)
        {_, txs_recv} = get_by_address_recv(pk, filters)
        txs = txs_sent ++ txs_recv

        txs = Enum.sort_by(txs, & &1.tx.nonce, filters.sort)
        txs = Enum.take(txs, filters.limit)

        cursor = case List.last(txs) do
            nil -> nil
            last_tx ->
                last_tx.tx.nonce
                |> Integer.to_string()
                |> String.pad_leading(20, "0")
        end

        {cursor, txs}
    end

    def get_by_address_sent(pk, filters) do
        pk = if byte_size(pk) != 48, do: Base58.decode(pk), else: pk

        {grep_func, start_key} = case filters.sort do
            :desc -> {&RocksDB.get_prev/3, (filters[:cursor] || :binary.copy("9", 20)) |> String.pad_leading(20, "0")}
            _ -> {&RocksDB.get_next/3, ""}
        end

        %{db: db, cf: cf} = :persistent_term.get({:rocksdb, Fabric})
        Enum.reduce_while(0..9_999_999, {nil, []}, fn(idx, {next_key, acc}) ->
            {next_key, value} = if idx == 0 do
                grep_func.("#{pk}:", start_key, %{db: db, cf: cf.tx_account_nonce, offset: filters.offset})
            else
                grep_func.("#{pk}:", next_key, %{db: db, cf: cf.tx_account_nonce})
            end

            if !next_key do {:halt, {next_key, acc}} else
                txu = API.TX.get(value)
                |> put_in([:metadata, :tx_event], :sent)
                action = hd(txu.tx.actions)
                cond do
                    !!filters[:contract] and filters.contract != action.contract -> {:cont, {next_key, acc}}
                    !!filters[:function] and filters.function != action.function -> {:cont, {next_key, acc}}
                    true ->
                        acc = acc ++ [txu]
                        if length(acc) >= filters.limit do
                            {:halt, {next_key, acc}}
                        else
                            {:cont, {next_key, acc}}
                        end
                end
            end
        end)
    end

    def get_by_address_recv(pk, filters) do
        pk = if byte_size(pk) != 48, do: Base58.decode(pk), else: pk

        {grep_func, start_key} = case filters.sort do
            :desc -> {&RocksDB.get_prev/3, (filters[:cursor] || :binary.copy("9", 20)) |> String.pad_leading(20, "0")}
            _ -> {&RocksDB.get_next/3, ""}
        end

        %{db: db, cf: cf} = :persistent_term.get({:rocksdb, Fabric})
        Enum.reduce_while(0..9_999_999, {nil, []}, fn(idx, {next_key, acc}) ->
            {next_key, value} = if idx == 0 do
                grep_func.("#{pk}:", start_key, %{db: db, cf: cf.tx_receiver_nonce, offset: filters.offset})
            else
                grep_func.("#{pk}:", next_key, %{db: db, cf: cf.tx_receiver_nonce})
            end
            if !next_key do {:halt, {next_key, acc}} else
                txu = API.TX.get(value)
                |> put_in([:metadata, :tx_event], :recv)
                action = hd(txu.tx.actions)
                cond do
                    !!filters[:contract] and filters.contract != action.contract -> {:cont, {next_key, acc}}
                    !!filters[:function] and filters.function != action.function -> {:cont, {next_key, acc}}
                    true ->
                        acc = acc ++ [txu]
                        if length(acc) >= filters.limit do
                            {:halt, {next_key, acc}}
                        else
                            {:cont, {next_key, acc}}
                        end
                end
            end
        end)
    end

    def submit(tx_packed) do
        result = TX.validate(tx_packed)
        if result[:error] == :ok do
            if tx_packed =~ "deploy" do
                txu = TX.unpack(tx_packed)
                action = hd(txu.tx.actions)
                if action.contract == "Contract" and action.function == "deploy" do
                    case BIC.Contract.validate(List.first(action.args)) do
                        %{error: :ok} ->
                            TXPool.insert(tx_packed)
                            NodeGen.broadcast(:txpool, :trainers, [[tx_packed]])
                            %{error: :ok, hash: Base58.encode(result.txu.hash)}
                        error -> error
                    end
                else
                    TXPool.insert(tx_packed)
                    NodeGen.broadcast(:txpool, :trainers, [[tx_packed]])
                    %{error: :ok, hash: Base58.encode(result.txu.hash)}
                end
            else
                TXPool.insert(tx_packed)
                NodeGen.broadcast(:txpool, :trainers, [[tx_packed]])
                %{error: :ok, hash: Base58.encode(result.txu.hash)}
            end
        else
            %{error: result.error}
        end
    end

    def format_tx_for_client(tx) do
        tx = Map.drop(tx, [:tx_encoded])
        tx = Map.put(tx, :signature, Base58.encode(tx.signature))
        tx = Map.put(tx, :hash, Base58.encode(tx.hash))
        tx = put_in(tx, [:tx, :signer], Base58.encode(tx.tx.signer))
        actions = Enum.map(tx.tx.actions, fn(a)->
            args = Enum.map(a.args, fn(arg)->
                cond do
                    !is_binary(arg) or Util.ascii?(arg) -> arg
                    true -> Base58.encode(arg)
                end
            end)
            Map.put(a, :args, args)
        end)
        tx = put_in(tx, [:tx, :actions], actions)
        tx = put_in(tx, [:metadata, :entry_hash], Base58.encode(tx.metadata.entry_hash))
    end
end
