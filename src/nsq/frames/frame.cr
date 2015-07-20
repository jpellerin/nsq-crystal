require "../logger"

module Nsq
  class Frame
    include Nsq::AttributeLogger
    @@log_attributes = [:connection]

    getter data, connection

    def initialize(@data, @connection)
    end
  end
end
