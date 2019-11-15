defmodule PingPong.Producer do
  @moduledoc """
  Sends pings to consumer processes
  """
  use GenServer

  alias PingPong.Consumer

  @initial %{current: 0}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def send_ping(server \\ __MODULE__) do
    GenServer.call(server, :send_ping)
  end

  def get_counts(server \\ __MODULE__) do
    GenServer.call(server, :get_counts)
  end

  def init(_args) do
    :net_kernel.monitor_nodes(true)
    {:ok, @initial}
  end

  def handle_call(:send_ping, _from, data) do
    GenServer.abcast(Consumer, {:ping, data.current+1, Node.self()})

    {:reply, :ok, %{data | current: data.current+1}}
  end

  def handle_call(:get_counts, _from, data) do
    {replies, _bad} = GenServer.multi_call(Consumer, :total_pings)
    map = Enum.into(replies, %{})
    {:reply, map, data}
  end

  # Don't remove me :)
  def handle_call(:flush, _, _) do
    {:reply, :ok, @initial}
  end

  def handle_cast({:catch_up, pid}, data) do
    GenServer.cast(pid, {:ping, data.current, Node.self()})
    {:noreply, data}
  end

  def handle_info({:nodeup, node}, data) do
    GenServer.cast({Consumer, node}, {:ping, data.current, Node.self()})
    {:noreply, data}
  end
  def handle_info(_msg, data) do
    # TODO - Fill me in l8r
    {:noreply, data}
  end
end

