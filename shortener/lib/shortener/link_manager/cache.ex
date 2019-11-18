defmodule Shortener.LinkManager.Cache do
  @moduledoc false
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def lookup(cache \\ __MODULE__, key) do
    # IO.inspect(:ets.tab2list(cache), label: "pre-lookup table dump [#{Node.self()}]")
    case :ets.lookup(cache, key) do
      [] -> {:error, :not_found}
      [{^key, link}] -> {:ok, link}
    end
  end

  def insert(cache \\ __MODULE__, key, value) do
    GenServer.call(cache, {:insert, key, value})
  end

  def broadcast_insert(cache \\ __MODULE__, key, value) do
    GenServer.abcast(Node.list(), cache, {:insert, key, value})
  end

  def flush(cache \\ __MODULE__) do
    GenServer.call(cache, :flush)
  end

  def init(_args) do
    table = :ets.new(__MODULE__, [:protected, :set, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  def handle_cast({:insert, key, value}, data) do
    :ets.insert(data.table, {key, value})
    # IO.inspect(:ets.tab2list(data.table), label: "post-cast-insert table dump [#{Node.self()}]")
    {:noreply, data}
  end

  def handle_call({:insert, key, value}, _from, data) do
    :ets.insert(data.table, {key, value})
    # IO.inspect(:ets.tab2list(data.table), label: "post-call-insert table dump [#{Node.self()}]")
    {:reply, :ok, data}
  end

  def handle_call(:flush, _from, data) do
    :ets.delete_all_objects(data.table)
    {:reply, :ok, data}
  end
end
