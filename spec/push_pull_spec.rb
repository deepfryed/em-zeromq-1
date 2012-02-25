require File.join(File.dirname(__FILE__), %w[spec_helper])

describe EventMachine::ZeroMQ do
  class EMTestPullHandler
    attr_reader :received
    def initialize
      @received = []
    end
    def on_readable(connection, messages)
      @received += messages
    end
  end

  it "Should instantiate a connection given valid opts" do
    pull_conn = nil
    run_reactor do
      pull_conn = SPEC_CTX.socket(ZMQ::PULL, EMTestPullHandler.new).bind(rand_addr)
    end
    pull_conn.should be_a(EventMachine::ZeroMQ::Connection)
  end

  describe "sending/receiving a single message via PUB/SUB" do
    before(:all) do
      results = {}
      @test_message = test_message = "TMsg#{rand(999)}"

      run_reactor(0.5) do

        address = rand_addr

        results[:pull_hndlr] = pull_hndlr = EMTestPullHandler.new
        pull_conn = SPEC_CTX.socket(ZMQ::PULL, pull_hndlr).bind(address)
        push_conn = SPEC_CTX.socket(ZMQ::PUSH).connect(address)

        push_conn.send test_message

        EM.add_timer(0.1) { results[:specs_ran] = true }
      end

      @results = results
    end

    it "should run completely" do
      @results[:specs_ran].should be_true
    end

    it "should receive the message intact" do
      @results[:pull_hndlr].received.should_not be_empty
      @results[:pull_hndlr].received.first.should == @test_message
    end
  end
end
