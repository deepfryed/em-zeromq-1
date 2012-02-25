require File.join(File.dirname(__FILE__), %w[spec_helper])

describe EventMachine::ZeroMQ do
  class EMTestSubHandler
    attr_reader :received
    def initialize
      @received = []
    end
    def on_readable(connection, messages)
      @received += messages
    end
  end

  it "Should instantiate a connection given valid opts" do
    sub_conn = nil
    address  = rand_addr

    run_reactor(1) do
      sub_conn = SPEC_CTX.socket(ZMQ::PUB, EMTestSubHandler.new).bind(address)
    end

    sub_conn.should be_a(EventMachine::ZeroMQ::Connection)
  end

  describe "sending/receiving a single message via PUB/SUB" do
    before(:all) do
      results = {}
      @test_message = test_message = "TMsg#{rand(999)}"

      run_reactor(0.5) do
        address = rand_addr

        results[:sub_hndlr] = pull_hndlr = EMTestSubHandler.new
        sub_conn = SPEC_CTX.socket(ZMQ::SUB, pull_hndlr) do |socket|
          socket.subscribe('')
          socket.bind(address)
        end

        pub_conn = SPEC_CTX.socket(ZMQ::PUB, EMTestSubHandler.new).connect(address)

        pub_conn.send test_message
        EM.add_timer(0.1) { results[:specs_ran] = true }
      end

      @results = results
    end

    it "should run completely" do
      @results[:specs_ran].should be_true
    end

    it "should receive one message" do
      @results[:sub_hndlr].received.length.should == 1
    end

    it "should receive the message intact" do
      @results[:sub_hndlr].received.first.should == @test_message
    end
  end
end
