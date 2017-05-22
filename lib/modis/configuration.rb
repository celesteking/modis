module Modis
  module Configuration
    def self.config
      return @@config if defined?(@@config)
      @@config = Struct.new(:namespace, :redis_opts).new(nil, {driver: :hiredis})
      @@config.redis_opts.merge!(Rails.application.config.modis.to_h) if Rails.application.config.modis
      @@config
    end
  end
end
