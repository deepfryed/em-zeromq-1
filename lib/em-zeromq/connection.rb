module EventMachine
  module ZeroMQ
    class Connection < EventMachine::Connection
      def initialize socket, handler
        @socket  = socket
        @handler = handler
      end

      # send a non blocking message
      # parts:  if only one argument is given a signle part message is sent
      #         if more than one arguments is given a multipart message is sent
      #
      # return: true is message was queued, false otherwise
      #
      def send(*parts)
        sent  = true
        parts = Array(parts[0]) if parts.size == 0

        # multipart
        parts[0...-1].each do |msg|
          sent = socket.send(msg, ZMQ::SNDMORE)
          if sent == false
            break
          end
        end

        if sent
          # all the previous parts were queued, send
          # the last one
          ret = socket.send(parts[-1])
          if ret < 0
            raise "Unable to send message: #{ZMQ::Util.error_string}"
          end
        else
          # error while sending the previous parts
          # register the socket for writability
          self.notify_writable = true
          sent = false
        end

        notify_readable()
        sent
      end

      # cleanup when ending loop
      def unbind
        detach_and_close
      end

      # Make this socket available for reads
      def register_readable
        # Since ZMQ is event triggered I think this is necessary
        if socket.readable?
          notify_readable
        end
        # Subscribe to EM read notifications
        self.notify_readable = true
      end

      # Trigger on_readable when socket is readable
      def register_writable
        # Subscribe to EM write notifications
        self.notify_writable = true
      end

      def notify_readable
        # Not sure if this is actually necessary. I suppose it prevents us
        # from having to to instantiate a ZMQ::Message unnecessarily.
        # I'm leaving this is because its in the docs, but it could probably
        # be taken out.
        return unless socket.readable?

        loop do
          msg_parts = []
          msg       = socket.recv
          if msg
            msg_parts << msg
            while socket.more_parts?
              msg = socket.recv
              if msg
                msg_parts << msg
              else
                raise "Multi-part message missing a message!"
              end
            end

            handler.on_readable(self, msg_parts) if handler && handler.respond_to?(:on_readable)
          else
            break
          end
        end
      end

      def notify_writable
        return unless socket.writable?

        # one a writable event is successfully received the socket
        # should be accepting messages again so stop triggering
        # write events
        self.notify_writable = false
        handler.on_writable(self) if handler && handler.respond_to?(:on_writable)
      end

    private
      attr_reader :socket, :handler
      # internal methods

      # Detaches the socket from the EM loop,
      # then closes the socket
      def detach_and_close
        detach
        socket.close
      end
    end # Connection
  end # ZeroMQ
end # EventMachine

