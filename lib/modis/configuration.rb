module Modis
  module Configuration
    mattr_reader :config

    def self.configure
      yield config
    end

    unless @@config
      @@config = Struct.new(:namespace, :redis_opts).new(nil, {driver: :hiredis})
      @@config.redis_opts.merge!(Rails.application.config.modis.to_h) if Rails.application.config.modis
    end
  end
end
