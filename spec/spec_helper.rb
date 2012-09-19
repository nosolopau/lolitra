$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)

require 'bundler'
Bundler.setup(:default, :test)

require "lolitra"

class TestBus
  def initialize
    @handlers = {}
    @unmarshallers = {}
  end

  def publish_directly(message_key, message_payload)
    publish(@unmarshallers[message_key].unmarshall(message_payload))
  end

  def publish(message)
    @handlers[message.class.name].each do |handler|
      handler.handle(message)
    end
  end

  def subscribe(message_class, handler_class)
    @unmarshallers[message_class.message_key] = message_class
    @handlers[message_class.name] ||= []
    @handlers[message_class.name] << handler_class
  end
end

