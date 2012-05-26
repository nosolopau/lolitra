require 'singleton'
require 'amqp'
require 'amqp/utilities/event_loop_helper'
require 'json'

module Lolitra
  module MessageHandler
    module Helpers
      def self.underscore(word)
        word.gsub!(/::/, '/')
        word.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
        word.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
        word.tr!("-", "_")
        word.downcase!
        word
      end
    end

    class NoHandlerMessageException < StandardError
      def initialize(handler, message)
        @handler_class = handler.name
        @message_class = message.class.name
      end

      def to_s
        "No handler (or starter if stateful) for message #{@message_class} in class #{@handler_class}"
      end
    end

    module MessageHandlerClass
      
      def self.extended(base)
        class << base
          attr_accessor :handlers
          attr_accessor :starters
          attr_accessor :is_stateful
        end
        base.handlers = {}
        base.starters = []
        base.is_stateful = false
      end

      def handle(message)
        #puts "#{self.name} try to handle new message #{message.class.name}"
        begin      
          get_handler(message).handle(message)
        rescue NoMethodError => e
          raise NoHandlerMessageException.new(self, message) if e.message == "undefined method `handle' for nil:NilClass"
          raise
        end
      end
      
      def publish(message)
        #TODO: IoC
        MessageHandlerManager.publish(message)
      end

      def handle_messages
        handlers.values.collect { |class_method_pair| class_method_pair[0] }
      end

    private
      def message_handler(message_class, id = :id)   
        message_class.new.send(id) #check if id exists for this class
        handlers.merge!(message_class.name => [message_class, get_method_by_class(message_class), id])
      end
     
      def started_by(message_class)
        starters << message_class.name
      end

      def search(message)
        id_method_name = handlers[message.class.name][2]
        send("find_by_#{id_method_name}", message.send(id_method_name))
      end

      def is_starter?(message)
        starters.include? message.class.name
      end

      def get_handler(message)
        if is_stateful?
          is_starter?(message) ? (search(message) || new) : search(message)
        else
          new
        end
      end

      def get_method_by_class(arg_class)
        MessageHandler::Helpers.underscore(arg_class.name).gsub("/","_").to_sym
      end

      def stateful(stateful_arg)
        self.is_stateful = stateful_arg
      end

      def is_stateful?
        is_stateful
      end
    end

    def self.included(base)
      base.send :extend, MessageHandlerClass
    end

    def handle(message)
      handler_method = self.class.handlers[message.class.name][1]
      raise "Can't handle message #{message.class}" unless handler_method
      self.send(handler_method, message)
    end

  end

  class MessageHandlerManager
    include Singleton

    attr_accessor :bus

    def self.bus=(new_bus)
      instance.bus = new_bus
    end

    def self.bus
      instance.bus
    end

    def self.register(handler_class)
      instance.register(handler_class)
    end
   
    def register(handler_class)
      register_message_handler(handler_class)    
    end

    def self.publish(message_instance)
      instance.publish(message_instance)
    end

    def publish(message_instance)
      bus.publish(message_instance)
    end
  private
    def register_message_handler(handler_class)
      handler_class.handle_messages.each do |message_class|
        bus.subscribe(message_class, handler_class)
      end
    end
  end

  module AmqpMessage

    module AmqpMessageClass
      def self.extended(base)
        class << base; attr_accessor :class_message_key; end
      end

      def message_key(key = nil)
        if (key)
          self.class_message_key = key      
        else
          self.class_message_key
        end
      end

      def unmarshall(message_json)
        hash = JSON.parse(message_json)
        self.new(hash)
      end

    end

    def self.included(base)
      base.send :extend, AmqpMessageClass
    end

    def initialize(hash={})   
      hash.each { |key, value| self.send("#{MessageHandler::Helper.underscore(key)}=", value) }
    end

    def marshall
      hash = {}
      self.instance_variables.each {|var| hash[var.to_s.delete("@")] = self.instance_variable_get(var) }
      JSON.generate(hash)
    end
  end

  module AmqpMessageHandler
    module AmqpMessageHandlerClass
      
      def self.extended(base)
        class << base
          attr_accessor :message_class_by_key
          alias_method :message_handler_without_class_by_key, :message_handler unless method_defined?(:message_handler_without_class_by_key)
          alias_method :message_handler, :message_handler_with_class_by_key
        end
        base.message_class_by_key = {}
      end

      def message_handler_with_class_by_key(message_class, id = :id)
        message_class_by_key.merge!({message_class.message_key => message_class})
        message_handler_without_class_by_key(message_class, id)
      end

    end

    def self.included(base)
      base.send :include, MessageHandler
      base.send :extend, AmqpMessageHandlerClass
    end

  end

  class AmqpBus
    attr_accessor :queue_prefix
    attr_accessor :connection
    attr_accessor :exchange

    def initialize(hash = {})
      params = hash.reject { |key, value| !value }
      raise "no :exchange specified" unless hash[:exchange]

      self.queue_prefix = hash[:queue_prefix]||""

      AMQP::Utilities::EventLoopHelper.run do
        self.connection = AMQP.connect(params)
        self.exchange = AMQP::Channel.new(self.connection).topic(params[:exchange], :durable => true)
      end
    end

    def subscribe(message_class, handler_class)
      EM.next_tick do
        channel = AMQP::Channel.new(self.connection)
        channel.prefetch(1).queue(queue_prefix + MessageHandler::Helpers.underscore(handler_class.name), :dureable => true).bind(self.exchange, :routing_key => message_class.message_key).subscribe do |info, payload|
          message_class_tmp = handler_class.message_class_by_key[info.routing_key]
          handler_class.handle(message_class_tmp.unmarshall(payload))
        end
      end
    end

    def publish(message)
      self.exchange.publish(message.marshall, :routing_key => message.class.message_key, :timestamp => Time.now.to_i)
    end

  end
end  
