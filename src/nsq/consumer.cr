require "./client_base"
require "./opts"

module Nsq
  class Consumer < ClientBase

    getter max_in_flight

    def initialize(opts = Opts.new)
      if opts[:nsqlookupd]
        @nsqlookupds = [opts[:nsqlookupd]].flatten
      else
        @nsqlookupds = [] of Opts
      end

      @topic = opts[:topic] || raise(ArgumentError, "topic is required")
      @channel = opts[:channel] || raise(ArgumentError, "channel is required")
      @max_in_flight = opts[:max_in_flight] || 1
      @discovery_interval = opts[:discovery_interval] || 60
      @msg_timeout = opts[:msg_timeout]

      # This is where we queue up the messages we receive from each connection
      @messages = opts[:queue] || Queue.new

      # This is where we keep a record of our active nsqd connections
      # The key is a string with the host and port of the instance (e.g.
      # "127.0.0.1:4150") and the key is the Connection instance.
      @connections = {} of String => Connection

      if !@nsqlookupds.empty?
        discover_repeatedly(
          nsqlookupds: @nsqlookupds,
          topic: @topic,
          interval: @discovery_interval
        )
      else
        # normally, we find nsqd instances to connect to via nsqlookupd(s)
        # in this case let's connect to an nsqd instance directly
        add_connection(opts[:nsqd] || "127.0.0.1:4150", max_in_flight: @max_in_flight)
      end

      at_exit{terminate}
    end

    # pop the next message off the queue
    def pop
      @messages.pop
    end

    # returns the number of messages we have locally in the queue
    def size
      @messages.size
    end

    private def add_connection(nsqd, options = Opts.new)
      super(nsqd, {
        topic: @topic,
        channel: @channel,
        queue: @messages,
        msg_timeout: @msg_timeout,
        max_in_flight: 1
      }.merge(options))
    end

    # Be conservative, but don't set a connection's max_in_flight below 1
    private def max_in_flight_per_connection(number_of_connections = @connections.length)
      [@max_in_flight / number_of_connections, 1].max
    end

    private def connections_changed
      redistribute_ready
    end

    private def redistribute_ready
      @connections.values.each do |connection|
        connection.max_in_flight = max_in_flight_per_connection
        connection.re_up_ready
      end
    end
  end
end
