require 'redis'
require 'connection_pool'

if Bundler.settings[:devplace] == 'local'
  require 'modis/version'
  require_dependency 'modis/configuration'
else
  require 'active_model'
  require 'active_support/all'
  require 'yaml'

  require 'modis/version'
  require 'modis/configuration'
  require 'modis/attribute'
  require 'modis/errors'
  require 'modis/persistence'
  require 'modis/transaction'
  require 'modis/finder'
  require 'modis/index'
  require 'modis/model'
  require 'modis/fun'
end

module Modis
  include Modis::Fun

  @mutex = Mutex.new

  class << self
    attr_accessor :connection_pool, :connection_pool_size,
                  :connection_pool_timeout
  end

  self.connection_pool_size = 5
  self.connection_pool_timeout = 5

  def self.connection_pool
    return @connection_pool if @connection_pool
    @mutex.synchronize do
      options = { size: connection_pool_size, timeout: connection_pool_timeout }
      @connection_pool = ConnectionPool.new(options) { Redis.new(Configuration.config.redis_opts) }
    end
  end

  def self.with_connection
    # puts 'Modis.with_connection called'
    if @redis_piped
      # puts 'reusing redis obj'
      yield(@redis_piped)
    else
      connection_pool.with { |redis| yield(redis) }
    end
  end

  def self.pipelined
    with_connection do |redis|
      redis.pipelined do
        begin
          raise(ArgumentError, 'double piped') if @redis_piped
          # puts 'acquiring piped obj'
          @redis_piped = redis
          yield(redis)
          # puts 'done exec pipelined block'
        ensure
          @redis_piped = nil
          # puts 'released piped obj'
        end
      end
    end
  end

end
