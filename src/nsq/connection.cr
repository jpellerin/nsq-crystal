require "json"
require "socket"
# FIXME what is this?
# require "timeout"

require "./frames/error"
require "./frames/message"
require "./frames/response"
require "./logger"

module Nsq
  class Connection
    include Nsq::AttributeLogger
    @@log_attributes = [:host, :port]

    getter :host
    getter :port
    property :max_in_flight
    getter :presumed_in_flight

    USER_AGENT = "nsq-ruby/#{Nsq::Version::STRING}"
    RESPONSE_HEARTBEAT = "_heartbeat_"
    RESPONSE_OK = "OK"


    def initialize(opts = {} of Symbol => String)
      @host = opts[:host] || (raise ArgumentError, "host is required")
      @port = opts[:port] || (raise ArgumentError, "host is required")
      @queue = opts[:queue]
      @topic = opts[:topic]
      @channel = opts[:channel]
      @msg_timeout = opts[:msg_timeout] || 60_000 # 60s
      @max_in_flight = opts[:max_in_flight] || 1

      if @msg_timeout < 1000
        raise ArgumentError, "msg_timeout cannot be less than 1000. it's in milliseconds."
      end

      # for outgoing communication
      @write_queue = Queue.new

      # For indicating that the connection has died.
      # We use a Queue so we don't have to poll. Used to communicate across
      # threads (from write_loop and read_loop to connect_and_monitor).
      @death_queue = Queue.new

      @connected = false
      @presumed_in_flight = 0

      open_connection
      start_monitoring_connection
    end


    def connected?
      @connected
    end


    # close the connection and don't try to re-open it
    def close
      stop_monitoring_connection
      close_connection
    end


    def sub(topic, channel)
      write "SUB #{topic} #{channel}\n"
    end


    def rdy(count)
      write "RDY #{count}\n"
    end


    def fin(message_id)
      write "FIN #{message_id}\n"
      decrement_in_flight
    end


    def req(message_id, timeout)
      write "REQ #{message_id} #{timeout}\n"
      decrement_in_flight
    end


    def touch(message_id)
      write "TOUCH #{message_id}\n"
    end


    def pub(topic, message)
      write ["PUB #{topic}\n", message.bytesize, message].pack("a*l>a*")
    end


    def mpub(topic, messages)
      body = messages.map do |message|
        [message.bytesize, message].pack("l>a*")
      end.join

      write ["MPUB #{topic}\n", body.bytesize, messages.size, body].pack("a*l>l>a*")
    end


    # Tell the server we are ready for more messages!
    def re_up_ready
      rdy(@max_in_flight)
      # assume these messages are coming our way. yes, this might not be the
      # case, but it's much easier to manage our RDY state with the server if
      # we treat things this way.
      @presumed_in_flight = @max_in_flight
    end

    private def cls
      write "CLS\n"
    end

    private def nop
      write "NOP\n"
    end

    private def write(raw)
      @write_queue.push(raw)
    end

    private def write_to_socket(raw)
      debug ">>> #{raw.inspect}"
      @socket.write(raw)
    end

    private def identify
      hostname = Socket.gethostname
      metadata = {
        client_id: Socket.gethostbyname(hostname).flatten.compact.first,
        hostname: hostname,
        feature_negotiation: true,
        heartbeat_interval: 30_000, # 30 seconds
        output_buffer: 16_000, # 16kb
        output_buffer_timeout: 250, # 250ms
        tls_v1: false,
        snappy: false,
        deflate: false,
        sample_rate: 0, # disable sampling
        user_agent: USER_AGENT,
        msg_timeout: @msg_timeout
      }.to_json
      write_to_socket ["IDENTIFY\n", metadata.length, metadata].pack("a*l>a*")

      # Now wait for the response!
      frame = receive_frame
      server = JSON.parse(frame.data)

      if @max_in_flight > server["max_rdy_count"]
        raise "max_in_flight is set to #{@max_in_flight}, server only supports #{server["max_rdy_count"]}"
      end

      @server_version = server["version"]
    end

    private def handle_response(frame)
      if frame.data == RESPONSE_HEARTBEAT
        debug "Received heartbeat"
        nop
      elsif frame.data == RESPONSE_OK
        debug "Received OK"
      else
        die "Received response we don't know how to handle: #{frame.data}"
      end
    end

    private def receive_frame
      if buffer = @socket.read(8)
        size, type = buffer.unpack("l>l>")
        size -= 4 # we want the size of the data part and type already took up 4 bytes
        data = @socket.read(size)
        frame_class = frame_class_for_type(type)
        return frame_class.new(data, self)
      end
    end

    FRAME_CLASSES = [Response, Error, Message]
    private def frame_class_for_type(type)
      raise "Bad frame type specified: #{type}" if type > FRAME_CLASSES.length - 1
      [Response, Error, Message][type]
    end

    private def decrement_in_flight
      @presumed_in_flight -= 1

      if server_needs_rdy_re_ups?
        # now that we're less than @max_in_flight we might need to re-up our RDY state
        threshold = (@max_in_flight * 0.2).ceil
        re_up_ready if @presumed_in_flight <= threshold
      end
    end

    private def start_read_loop
      @read_loop_thread ||= Thread.new{read_loop}
    end

    private def stop_read_loop
      @read_loop_thread.kill if @read_loop_thread
      @read_loop_thread = nil
    end

    private def read_loop
      loop do
        frame = receive_frame
        if frame.is_a?(Response)
          handle_response(frame)
        elsif frame.is_a?(Error)
          error "Error received: #{frame.data}"
        elsif frame.is_a?(Message)
          debug "<<< #{frame.body}"
          @queue.push(frame) if @queue
        else
          raise "No data from socket"
        end
      end
    rescue ex : Exception
      die(ex)
    end

    private def start_write_loop
      @write_loop_thread ||= Thread.new{write_loop}
    end

    private def stop_write_loop
      @stop_write_loop = true
      @write_loop_thread.join(1) if @write_loop_thread
      @write_loop_thread = nil
    end

    private def write_loop
      @stop_write_loop = false
      data = nil
      loop do
        data = @write_queue.pop
        write_to_socket(data)
        break if @stop_write_loop && @write_queue.size == 0
      end
    rescue ex : Exception
      # requeue PUB and MPUB commands
      if data =~ /^M?PUB/
        debug "Requeueing to write_queue: #{data.inspect}"
        @write_queue.push(data)
      end
      die(ex)
    end

    # Waits for death of connection
    private def start_monitoring_connection
      @connection_monitor_thread ||= Thread.new{monitor_connection}
      @connection_monitor_thread.abort_on_exception = true
    end

    private def stop_monitoring_connection
      @connection_monitor_thread.kill if @connection_monitor_thread
      @connection_monitor = nil
    end

    private def monitor_connection
      loop do
        # wait for death, hopefully it never comes
        cause_of_death = @death_queue.pop
        warn "Died from: #{cause_of_death}"

        debug "Reconnecting..."
        reconnect
        debug "Reconnected!"

        # clear all death messages, since we're now reconnected.
        # we don't want to complete this loop and immediately reconnect again.
        @death_queue.clear
      end
    end

    # close the connection if it's not already closed and try to reconnect
    # over and over until we succeed!
    private def reconnect
      close_connection
      with_retries do
        open_connection
      end
    end

    private def open_connection
      @socket = TCPSocket.new(@host, @port)
      # write the version and IDENTIFY directly to the socket to make sure
      # it gets to nsqd ahead of anything in the `@write_queue`
      write_to_socket "  V2"
      identify

      start_read_loop
      start_write_loop
      @connected = true

      # we need to re-subscribe if there's a topic specified
      if @topic
        debug "Subscribing to #{@topic}"
        sub(@topic, @channel)
        re_up_ready
      end
    end

    # closes the connection and stops listening for messages
    private def close_connection
      cls if connected?
      stop_read_loop
      stop_write_loop
      @socket = nil
      @connected = false
    end

    # this is called when there's a connection error in the read or write loop
    # it triggers `connect_and_monitor` to try to reconnect
    private def die(reason)
      @connected = false
      @death_queue.push(reason)
    end

    # Retry the supplied block with exponential backoff.
    #
    # Borrowed liberally from:
    # https://github.com/ooyala/retries/blob/master/lib/retries.rb
    private def with_retries(&block)
      base_sleep_seconds = 0.5
      max_sleep_seconds = 300 # 5 minutes

      # Let's do this thing
      attempts = 0
      start_time = Time.now

      begin
        attempts += 1
        return block.call(attempts)

      rescue ex : Errno|Timeout::Error

        # FIXME check errno

        raise ex if attempts >= 100

        # The sleep time is an exponentially-increasing function of base_sleep_seconds.
        # But, it never exceeds max_sleep_seconds.
        sleep_seconds = [base_sleep_seconds * (2 ** (attempts - 1)), max_sleep_seconds].min
        # Randomize to a random value in the range sleep_seconds/2 .. sleep_seconds
        sleep_seconds = sleep_seconds * (0.5 * (1 + rand()))
        # But never sleep less than base_sleep_seconds
        sleep_seconds = [base_sleep_seconds, sleep_seconds].max

        warn "Failed to connect: #{ex}. Retrying in #{sleep_seconds.round(1)} seconds."

        snooze(sleep_seconds)

        retry
      end
    end

    # Se we can stub for testing and reconnect in a tight loop
    private def snooze(t)
      sleep(t)
    end

    private def server_needs_rdy_re_ups?
      # versions less than 0.3.0 need RDY re-ups
      # see: https://github.com/bitly/nsq/blob/master/ChangeLog.md#030---2014-11-18
      major, minor, patch = @server_version.split(".").map(&:to_i)
      major == 0 && minor <= 2
    end

  end
end
