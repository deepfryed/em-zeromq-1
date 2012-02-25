require File.join(File.dirname(__FILE__), %w[spec_helper])

describe 'Context' do
  before do
    @ctx = EM::ZeroMQ::Context.new(1)
  end

  it 'can be created with a context' do
    zmq_ctx = ZMQ::Context.new(1)
    ctx     = EM::ZeroMQ::Context.attach(zmq_ctx)
    ctx.instance_variable_get('@zmq_context').should == zmq_ctx
  end

  it 'can create socket' do
    EM.run do
      s = @ctx.socket(ZMQ::ROUTER)
      s.instance_variable_get('@zmq_socket').name.should == 'ROUTER'
      EM.stop
    end
  end
end

