module Modis
  module Fun
    def self.included(base)
      base.extend ClassMethods
    end

    module ClassMethods
      mattr_reader(:scripts, instance_reader: false){ {} }

      def script(hash, keys, args = [])
        Modis.with_connection { |redis| redis.evalsha(hash, keys, args) }
      end

      def load_script(path)
        file = File.basename(path)
        if (hash = @@scripts[file])
          # we know hash
          if Modis.with_connection { |redis| redis.script(:exists, hash) }
            return hash
          end
        end

        # otherwise, [re]load it
        @@scripts[file] = Modis.with_connection do |redis|
          redis.script('load', File.read(path))
        end
      end
    end
  end
end
