module EventMachine
  module ZeroMQ
    class Socket
      attr_accessor :on_readable, :on_writable, :handler
      attr_reader   :zmq_socket, :type

      READABLES = [ ZMQ::SUB, ZMQ::PULL, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]
      WRITABLES = [ ZMQ::PUB, ZMQ::PUSH, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]

      # Public API
      def initialize(socket, handler = nil)
        @zmq_socket = socket
        @type, @fd  = getsockopt(ZMQ::TYPE), getsockopt(ZMQ::FD)
        @connection = attach(handler) if handler
      end

      def attach handler
        @connection.detach if @connection
        @connection = EM.watch(@fd, Connection, self, handler).tap do |s|
          s.register_readable if READABLES.include?(type)
          s.register_writable if WRITABLES.include?(type)
        end
      end

      def self.map_sockopt(name, option)
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


      def bind(address)
        zmq_socket.bind(address)
        @connection ? @connection : attach(nil)
      end

      def connect(address)
        zmq_socket.connect(address)
        @connection ? @connection : attach(nil)
      end

      def close
        zmq_socket.close
      end

      def subscribe(what = '')
        raise "only valid on sub socket type (was #{zmq_socket.name})" unless type == ZMQ::SUB
        setsockopt(ZMQ::SUBSCRIBE, what)
      end

      def unsubscribe(what)
        raise "only valid on sub socket type (was #{zmq_socket.name})" unless type == ZMQ::SUB
        setsockopt(ZMQ::UNSUBSCRIBE, what)
      end

      def send message, flags = 0
        zmq_socket.send_string(message, ZMQ::NOBLOCK | flags)
      end

      def recv
        message = ZMQ::Message.new
        success = zmq_socket.recv(message, ZMQ::NOBLOCK)
        success ? message.copy_out_string : nil
      end

      def getsockopt(opt)
        ret = []
        rc  = zmq_socket.getsockopt(opt, ret)
        unless ZMQ::Util.resultcode_ok?(rc)
          raise ZMQOperationFailed, "getsockopt: #{ZMQ::Util.error_string}"
        end

        (ret.size == 1) ? ret[0] : ret
      end

      def setsockopt(opt, value)
        zmq_socket.setsockopt(opt, value)
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
    end
  end
end
