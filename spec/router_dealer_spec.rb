require File.join(File.dirname(__FILE__), %w[spec_helper])

describe EventMachine::ZeroMQ do
  class EMTestRouterHandler
    attr_reader :received
    def initialize
      @received = []
    end
    def on_writable(connection)
    end
    def on_readable(connection, messages)
      @received += messages
    end
  end

  class EMTestDealerHandler
    attr_reader :received
    def initialize(&block)
      @received = []
      @on_writable_callback = block
    end
    def on_writable(connection)
      @on_writable_callback.call(connection) if @on_writable_callback
    end
    def on_readable(connection, messages)
      @received += messages
      connection.send('', "re:#{messages.join}")
    end
  end

  it "Should instantiate a connection given valid opts for Router/Dealer" do
    router_conn = nil
    run_reactor(1) do
      router_conn = SPEC_CTX.socket(ZMQ::ROUTER, EMTestRouterHandler.new).bind(rand_addr)
    end
    router_conn.should be_a(EventMachine::ZeroMQ::Connection)
  end

  describe "sending/receiving a single message via Router/Dealer" do
    before(:all) do
      results = {}
      @test_message = test_message = "M#{rand(999)}"

      run_reactor(2) do
        results[:dealer_hndlr] = dealer_hndlr = EMTestDealerHandler.new
        results[:router_hndlr] = router_hndlr = EMTestRouterHandler.new

        addr = rand_addr
        dealer_conn = SPEC_CTX.socket(ZMQ::DEALER, dealer_hndlr) do |socket|
          socket.identity = "dealer1"
          socket.bind(addr)
        end

        router_conn = SPEC_CTX.socket(ZMQ::ROUTER, router_hndlr) do |socket|
          socket.identity = "router1"
          socket.connect(addr)
        end

        EM.add_timer(0.1) do
          router_conn.send('dealer1','', test_message)
        end

        EM.add_timer(0.2) do
          results[:specs_ran] = true
        end
      end

      @results = results
    end

    it "should run completely" do
      @results[:specs_ran].should be_true
    end

    it "should receive the message intact on the dealer" do
      @results[:dealer_hndlr].received.should_not be_empty
      @results[:dealer_hndlr].received.last.should == @test_message
    end

    it "the router should be echoed its original message" do
      @results[:router_hndlr].received.should_not be_empty
      @results[:router_hndlr].received.last.should == "re:#{@test_message}"
    end
  end
end
