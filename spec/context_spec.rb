require File.join(File.dirname(__FILE__), %w[spec_helper])

describe 'Context' do
  before do
    @ctx = EM::ZeroMQ::Context.new(1)
  end

  it 'can be created with a context' do
    zmq_context = ZMQ::Context.new(1)
    context     = EM::ZeroMQ::Context.attach(zmq_context)
    context.zmq_context.should == zmq_context
  end

  it 'can create socket' do
    EM.run do
      socket = @ctx.socket(ZMQ::ROUTER)
      socket.zmq_socket.name.should == 'ROUTER'
      EM.stop
    end
  end
end

