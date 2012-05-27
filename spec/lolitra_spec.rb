require 'spec_helper.rb'

class TestMessage
  include Lolitra::Message

  message_key "test1"

  def id
    1
  end
end

class TestMessage1
  include Lolitra::Message

  message_key "test2"

  def id
    2
  end
end

class TestMessageHandler
  include Lolitra::MessageHandler

  started_by TestMessage
  stateful true

  def self.find_by_id(id)
    nil
  end

  def test_message(message)
    "handled"
  end
end

class TestMessageHandler1
  include Lolitra::MessageHandler

  started_by TestMessage1
  message_handler TestMessage

  stateful true

  def self.find_by_id(id)
    nil
  end

  def test_message(message)
    "handled"
  end
  
  def test_message1(message)
    "handled1"
  end

end

class TestMessageHandlerAmqp
  include Lolitra::MessageHandler

  message_handler TestMessage
  message_handler TestMessage1

  stateful false

  def test_message(message)
    EM.stop { exit } 
  end
end


describe Lolitra::MessageHandler,'#create_handler' do
  it "returns old instance of the handler for the same message id" do
    handler = TestMessageHandler.new
    TestMessageHandler.should_receive(:new).exactly(1).and_return(handler)
    TestMessageHandler.handle(TestMessage.new)
    TestMessageHandler.should_receive(:find_by_id).at_least(:once).and_return(handler)
    TestMessageHandler.handle(TestMessage.new)
    TestMessageHandler.handle(TestMessage.new)
  end

  it "create a new handler if the message starter has different id" do
    handler1 = TestMessageHandler.new   
    message1 = TestMessage.new
    message2 = TestMessage.new
    message2.stub(:id => 2)
    
    TestMessageHandler.should_receive(:new).exactly(2).and_return(handler1)
    TestMessageHandler.handle(message1)
    TestMessageHandler.handle(message2)
  end
end

describe Lolitra::MessageHandler, '#handle_message' do
  it "handle message when message arrives" do
    message_handler = TestMessageHandler1.new
    TestMessageHandler1.stub(:find_by_id).and_return(nil, message_handler)
    TestMessageHandler1.handle(TestMessage1.new).should eq "handled1"
    TestMessageHandler1.handle(TestMessage.new).should eq "handled"
  end
end

describe Lolitra::MessageHandler, "#handle_message" do
  it "handle non starter message thow execption" do
    expect { TestMessageHandler1.handle(TestMessage.new) }.to raise_error(Lolitra::MessageHandler::NoHandlerMessageException)
  end
end

describe Lolitra::MessageHandler, '#publish' do
  it "can send message to the bus" do
    message = TestMessage.new
    bus = TestBus.new
    Lolitra::MessageHandlerManager.bus = bus
    Lolitra::MessageHandlerManager.register(TestMessageHandler)


    TestMessageHandler.should_receive(:handle).with(message)
    TestMessageHandler.publish(message)
  end
end

describe Lolitra::MessageHandlerManager, '#publish' do
  it "can send message to the bus" do
    bus = TestBus.new
    Lolitra::MessageHandlerManager.bus = bus
    Lolitra::MessageHandlerManager.register(TestMessageHandler)

    message = TestMessage.new

    TestMessageHandler.should_receive(:handle).with(message)

    Lolitra::MessageHandlerManager.publish(message)
  end
end

describe Lolitra::MessageHandlerManager, '#handle_message' do
  it "handle message with the correct handler" do

    bus = TestBus.new
    
    Lolitra::MessageHandlerManager.bus = bus

    Lolitra::MessageHandlerManager.register(TestMessageHandler)
    Lolitra::MessageHandlerManager.register(TestMessageHandler1)

    message = TestMessage.new
    message1 = TestMessage1.new

    TestMessageHandler.should_receive(:handle).with(message)
    TestMessageHandler.should_not_receive(:handle).with(message1)

    TestMessageHandler1.should_receive(:handle).with(message)
    TestMessageHandler1.should_receive(:handle).with(message1)

    Lolitra::MessageHandlerManager.publish(message)
    Lolitra::MessageHandlerManager.publish(message1)
  end
end

describe Lolitra::Message, '#message_key' do
  it "message_key has constant key for a class" do
    TestMessage.message_key.should eq "test1"
    TestMessage1.message_key.should eq "test2"
    TestMessage.message_key.should eq "test1"
  end
end

describe Lolitra::MessageHandler, '#message_class_by_key' do
  it "should return the message_class that belongs to key" do
    TestMessageHandler1.handlers[TestMessage.message_key][0].name.should eq "TestMessage" 
    TestMessageHandler1.handlers[TestMessage1.message_key][0].name.should eq "TestMessage1" 
  end
end

describe Lolitra::AmqpBus do
  it "should recive message when publish a message" do
    Lolitra::MessageHandlerManager.bus = Lolitra::AmqpBus.new(:host => "127.0.0.1", :exchange => "test_exchange")
    Lolitra::MessageHandlerManager.register(TestMessageHandlerAmqp)

    TestMessageHandlerAmqp.should_receive(:handle) do |message|
      message.should be_an_instance_of TestMessage
      EM.stop { exit }
    end

    Lolitra::MessageHandlerManager.publish(TestMessage.new)

    EM.next_tick{ EM.add_timer(60) { EM.stop { exit } }}
    AMQP::Utilities::EventLoopHelper.eventmachine_thread.join
  end
end

describe Lolitra::AmqpBus do
  it "should return a unable to connect when specify a wrong amqp host" do
    expect { Lolitra::AmqpBus.new(:host => "192.168.123.123") }.to raise_error(Exception) #TODO
  end
end
