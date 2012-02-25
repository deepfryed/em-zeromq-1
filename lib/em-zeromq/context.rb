#
# different ways to create a socket:
# ctx.bind(:xreq, 'tcp://127.0.0.1:6666')
# ctx.bind('xreq', 'tcp://127.0.0.1:6666')
# ctx.bind(ZMQ::XREQ, 'tcp://127.0.0.1:6666')
#
module EventMachine
  module ZeroMQ
    class Context
      attr_reader :zmq_context

      def initialize threads
        @zmq_context = ZMQ::Context.new(threads)
      end

      def self.attach context
        allocate.tap {|instance| instance.instance_variable_set(:@zmq_context, context)}
      end

      ##
      # Create a socket in this context.
      #
      # @param [Integer] socket_type One of ZMQ::REQ, ZMQ::REP, ZMQ::PULL, ZMQ::PUSH,
      #   ZMQ::ROUTER, ZMQ::DEALER
      #
      # @param [Object] handler
      #   1. An object which responds to on_readable(connection, parts) or respond to on_writeable(connection)
      #   2. Subclass of EM::ZeroMQ::Connection
      #
      def socket type, handler = nil, *args
        socket = Socket.new(@zmq_context.socket(type), handler, *args)
        block_given? ? yield(socket) : socket
      end
    end # Context
  end # ZeroMQ
end # EventMachine
