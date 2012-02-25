module EventMachine
  module ZeroMQ
    class Socket
      attr_reader :zmq_socket, :type

      READABLES = [ ZMQ::SUB, ZMQ::PULL, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]
      WRITABLES = [ ZMQ::PUB, ZMQ::PUSH, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]

      # Public API
      def initialize socket, handler = nil, *args
        @zmq_socket = socket
        @type, @fd  = getsockopt(ZMQ::TYPE), getsockopt(ZMQ::FD)
        @connection = attach(handler, *args) if handler
      end

      # allows you to attach
      # 1. Any object that responds to :on_readable & :on_writable
      # 2. A subclass of EM::ZeroMQ::Connection
      def attach handler, *args
        @connection.detach if @connection

        if handler.kind_of?(Class) && handler < Connection
          klass = handler
        else
          klass = DefaultHandler
          args << handler
        end

        @connection = EM.watch(@fd, klass, self, *args).tap do |s|
          s.register_readable if READABLES.include?(type)
          s.register_writable if WRITABLES.include?(type)
        end
      end

      def self.map_sockopt name, option
        define_method(name) do
          getsockopt(option)
        end

        define_method("#{name}=") do |value|
          setsockopt(option, value)
        end
      end

      map_sockopt :hwm,               ZMQ::HWM
      map_sockopt :swap,              ZMQ::SWAP
      map_sockopt :affinity,          ZMQ::AFFINITY
      map_sockopt :identity,          ZMQ::IDENTITY
      map_sockopt :sndbuf,            ZMQ::SNDBUF
      map_sockopt :rcvbuf,            ZMQ::RCVBUF

      # pgm
      map_sockopt :rate,              ZMQ::RATE
      map_sockopt :recovery_ivl,      ZMQ::RECOVERY_IVL
      map_sockopt :mcast_loop,        ZMQ::MCAST_LOOP

      map_sockopt :linger,            ZMQ::LINGER
      map_sockopt :reconnect_ivl,     ZMQ::RECONNECT_IVL
      map_sockopt :reconnect_ivl_max, ZMQ::RECONNECT_IVL_MAX
      map_sockopt :backlog,           ZMQ::BACKLOG

      def bind address
        zmq_socket.bind(address)
        @connection ? @connection : attach(nil)
      end

      def connect address
        zmq_socket.connect(address)
        @connection ? @connection : attach(nil)
      end

      def close
        zmq_socket.close
      end

      def subscribe what = ''
        raise "only valid on sub socket type (was #{zmq_socket.name})" unless type == ZMQ::SUB
        setsockopt(ZMQ::SUBSCRIBE, what)
      end

      def unsubscribe what = ''
        raise "only valid on sub socket type (was #{zmq_socket.name})" unless type == ZMQ::SUB
        setsockopt(ZMQ::UNSUBSCRIBE, what)
      end

      def send message, flags = 0
        zmq_socket.send_string(message, ZMQ::NOBLOCK | flags)
      end

      def recv
        message = ZMQ::Message.new
        rc      = zmq_socket.recv(message, ZMQ::NOBLOCK)
        ZMQ::Util.resultcode_ok?(rc) ? message.copy_out_string : nil
      end

      def getsockopt option
        value = []
        rc    = zmq_socket.getsockopt(option, value)
        unless ZMQ::Util.resultcode_ok?(rc)
          raise ZMQOperationFailed, "getsockopt: #{ZMQ::Util.error_string}"
        end
        (value.size == 1) ? value[0] : value
      end

      def setsockopt option, value
        rc = zmq_socket.setsockopt(option, value)
        unless ZMQ::Util.resultcode_ok?(rc)
          raise ZMQOperationFailed, "setsockopt: #{ZMQ::Util.error_string}"
        end
      end

      def more_parts?
        zmq_socket.more_parts?
      end

      def readable?
        (getsockopt(ZMQ::EVENTS) & ZMQ::POLLIN) == ZMQ::POLLIN
      end

      def writable?
        return true
        # ZMQ::EVENTS has issues in ZMQ HEAD, we'll ignore this till they're fixed
        # (getsockopt(ZMQ::EVENTS) & ZMQ::POLLOUT) == ZMQ::POLLOUT
      end
    end # Socket

    class DefaultHandler < Connection
      def initialize socket, handler
        @handler = handler
        super
      end

      def on_readable messages
        @handler.on_readable(self, messages) if @handler.respond_to?(:on_readable)
      end

      def on_writable
        @handler.on_writable(self) if @handler.respond_to?(:on_writable)
      end
    end # DefaultHandler
  end # ZeroMQ
end # EventMachine
