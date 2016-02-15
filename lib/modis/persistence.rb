module Modis
  module Persistence
    def self.included(base)
      base.extend ClassMethods
      base.instance_eval do
        class << self
          attr_reader :sti_child
          alias_method :sti_child?, :sti_child
        end
      end
    end

    module ClassMethods
      # :nodoc:
      def bootstrap_sti(parent, child)
        child.instance_eval do
          parent.instance_eval do
            class << self
              attr_accessor :sti_parent
            end
            attribute :type, :string unless attributes.key?('type')
          end

          @sti_child = true
          @sti_parent = parent

          bootstrap_attributes(parent)
          bootstrap_indexes(parent)
        end
      end

      def namespace
        return sti_parent.namespace if sti_child?
        @namespace ||= name.split('::').map(&:underscore).join(':')
      end

      def namespace=(value)
        @namespace = value
        @absolute_namespace = nil
      end

      def absolute_namespace
        @absolute_namespace ||= [Modis.config.namespace, namespace].compact.join(':')
      end

      def key_for(id)
        "#{absolute_namespace}:#{id}"
      end

      def create(attrs)
        model = new(attrs)
        model.save
        model
      end

      def create!(attrs)
        model = new(attrs)
        model.save!
        model
      end

      def deserialize(record)
        values = record.values
        keys = record.keys

        index = 0
        while index < values.size # Optimized: https://github.com/rails/rails/pull/12065
          record[keys[index]] = values[index]
          index+=1
        end
        record
      end
    end

    def persisted?
      true
    end

    def key
      new_record? ? nil : self.class.key_for(id)
    end

    def new_record?
      defined?(@new_record) ? @new_record : true
    end

    def save(args = {})
      create_or_update(args)
    rescue Modis::RecordInvalid
      false
    end

    def save!(args = {})
      create_or_update(args) || (raise RecordNotSaved)
    end

    def destroy
      self.class.transaction do |redis|
        run_callbacks :destroy do
          redis.pipelined do
           # remove_from_indexes(redis)
            redis.srem(self.class.key_for(:all), id)
            redis.del(key)
          end
        end
      end
    end

    def reload
      new_attributes = Modis.with_connection { |redis| self.class.attributes_for(redis, id) }
      initialize(new_attributes)
      self
    end

    def update_attribute(name, value)
      assign_attributes(name => value)
      save(validate: false)
    end

    def update_attributes(attrs)
      assign_attributes(attrs)
      save
    end

    def update_attributes!(attrs)
      assign_attributes(attrs)
      save!
    end

    private

    def coerce_for_persistence(value)
      if (value.instance_of?(Time))
        # Persist as ISO8601 UTC with milliseconds
        return value.utc.iso8601 3
      end

      if (value.instance_of?(Hash))
        return JSON.dump(value)
      end

      if value.nil?
        return "nil"
      end

      value.to_s
    end

    def create_or_update(args = {})
      validate(args)
      future = persist(args[:yaml_sucks],args[:skip_index])

      #if future && (future == :unchanged || future.value == 'OK')
        reset_changes
        @new_record = false
        true
      #else
      #  false
      #end
    end

    def validate(args)
      skip_validate = args.key?(:validate) && args[:validate] == false
      return if skip_validate || valid?
      raise Modis::RecordInvalid, errors.full_messages.join(', ')
    end

    def persist(persist_all,skip_index=false)
      future = nil
      set_id if new_record?
      callback = new_record? ? :create : :update


      run_callbacks :save do
        run_callbacks callback do
          attrs = coerced_attributes(persist_all)
          if(new_record? && !skip_index) # if new record and we need to index it
            self.class.transaction do |redis|
              future = attrs.empty? ? :unchanged : redis.hmset(self.class.key_for(id), attrs)
              redis.sadd(self.class.key_for(:all), id)
            end
          else
            Modis.with_connection do |redis|
              future = attrs.empty? ? :unchanged : redis.hmset(self.class.key_for(id), attrs)
            end
          end
        end
      end

      future
    end

    def coerced_attributes(persist_all) # rubocop:disable Metrics/AbcSize
      attrs = []

      if new_record? || persist_all
        attributes.each do |k, v|
          if (self.class.attributes[k][:default] || nil) != v
            attrs << k << coerce_for_persistence(v)
          end
        end
      else
        changed_attributes.each do |k, v|
          attrs << k << coerce_for_persistence(attributes[k])
        end
      end

      puts "ATTRS:", *attrs
      attrs
    end

    def set_id
      Modis.with_connection do |redis|

        self.id = redis.incr("#{self.class.absolute_namespace}_id_seq")
      end
    end
  end
end
