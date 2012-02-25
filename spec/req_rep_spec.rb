require File.join(File.dirname(__FILE__), %w[spec_helper])

describe EventMachine::ZeroMQ do
  class EMTestSubHandler
    attr_reader :received
    def initialize
      @received = []
    end
    def on_readable(connection, messages)
      @received += messages
      connection.send('ok')
    end
  end

  describe "sending/receiving a single message via REQ/REP" do
    before(:all) do
      @results      = {}
      @test_message = "TMsg#{rand(999)}"

      run_reactor(0.5) do
        address = rand_addr

        @results[:rep_handler] = rep_handler = EMTestSubHandler.new
        rep_conn = SPEC_CTX.socket(ZMQ::REP, rep_handler) do |socket|
          socket.bind(address)
        end

        @results[:req_handler] = req_handler = EMTestSubHandler.new
        req_conn = SPEC_CTX.socket(ZMQ::REQ, req_handler) do |socket|
          socket.connect(address)
        end

        req_conn.send(@test_message)
        EM.add_timer(0.1) { @results[:specs_ran] = true }
      end
    end

    it "should run completely" do
      @results[:specs_ran].should be_true
    end

    it "rep handler should receive message" do
      @results[:rep_handler].received.first.should == @test_message
    end

    it "req handler should receive reply" do
      @results[:req_handler].received.first.should == 'ok'
    end
  end
end
