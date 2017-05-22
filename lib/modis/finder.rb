module Modis
  module Finder
    BATCH_SIZE = 500

    def self.included(base)
      base.extend ClassMethods
    end

    module ClassMethods
      # @param [Array<Integer>] ids
      # @param [Hash] opts
      # @option [Array<String>] :include Also fetch these models, by primary key
      # @option [String]        :noerror Don't error out on missing records
      # @return [Array<Model>]
      # @return [Hash{modelself: Array<>, model2: Array<>, ...}] When additional models are requested via :include
      def find(*ids)
        opts = ids.extract_options!

        models = [self]

        if (extra_models = opts[:include])
          models.concat([extra_models].flatten.map{|mdl| mdl.to_s.camelize.constantize })
        end

        raise RecordNotFound, "Couldn't find #{name} without an ID" if ids.empty?

        if ids.size == 1 and (Array === ids.first or Range === ids.first)
          ids = ids.first
        end

        # want_multiple = (Array === ids or Range === ids or extra_models)

        records_matrix = Modis.pipelined do |redis|
          ids.each do |id|
            models.each do |mdl|
              record_for(redis, id, mdl)
            end
          end
        end

        model_table = multi_records_to_models(records_matrix, ids, models, opts[:noerror])

        if extra_models
          model_table
        elsif ids.size == 1
          model_table.first.first
        else
          model_table.first
        end
      end

      def all
        records = Modis.with_connection do |redis|
          ids = redis.smembers(key_for(:all))
          redis.pipelined do
            ids.map { |id| record_for(redis, id) }
          end
        end

        records_to_models_compact(records)
      end

      # Retrieve records in batch
      def scan(batch_size = 100, &block)
        sscan(key_for(:all), count: batch_size) do |red, ids, processed|
          records = Modis.pipelined do |redis|
            ids.each { |id| record_for(redis, id) }
          end

          yield(red, records_to_models(records), processed)
        end
      end

      def count
        Modis.with_connection do |redis|
          redis.scard(key_for(:all))
        end
      end

      def exists?(*ids)
        ids = ids.first if ids.size == 1 and Array === ids.first

        ret = Modis.pipelined do |redis|
          ids.map do |id|
            redis.exists(key_for(id))
          end
        end
        ids.size == 1 ? ret.first : ret
      end
      alias_method :exist?, :exists?

      def attributes_for(redis, id)
        raise RecordNotFound, "Couldn't find #{name} without an ID" if id.nil?

        attributes = deserialize(record_for(redis, id))

        unless attributes['id'].present?
          raise RecordNotFound, "Couldn't find #{name} with id=#{id}"
        end

        attributes
      end

      private
      def records_to_models(records)
        records.map do |record|
          model_for(deserialize(record)) unless record.blank?
        end
      end

      def records_to_models_compact(records)
        records_to_models(records).compact
      end

      def multi_records_to_models(matrix, ids, models = [self], include_empty = false)
        models_size = models.size
        model_tbl = Array.new(models_size){ [] }
        idx = 0; matrix_chunk_count = matrix.size / models_size
        while idx < matrix_chunk_count
          models.each_with_index do |model, mdl_idx|
            record = matrix[idx * models_size + mdl_idx]

            model_tbl[mdl_idx] <<
                if record.empty?
                  include_empty ? nil : raise(RecordNotFound, "Couldn't find #{model.name} with key '#{ids.each_with_index.find{|_, i| i == idx}.first}'")
                else
                  model.new(deserialize(record), new_record: false)
                end
          end
          idx += 1
        end

        model_tbl
      end

      def model_for(attributes)
        model_class(attributes).new(attributes, new_record: false)
      end

      def record_for(redis, id, model = self)
        redis.hgetall(model.key_for(id))
      end

      def model_class(record)
        return self if record["type"].blank?
        record["type"].constantize
      end

      def sscan(key, options = {}, &block)
        Modis.with_connection do |redis|
          options.reverse_merge!(count: BATCH_SIZE)
          processed = 0
          cursor = 0
          while true
            cursor, keys = redis.sscan(key, cursor, options)
            if (keys_size = keys.size) > 0
              break unless yield(redis, keys, (processed += keys_size))
            end
            break if cursor == '0'
          end
          processed
        end
      end
    end
  end
end
