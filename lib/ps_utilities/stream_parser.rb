require 'oj'
require 'piperator'

# https://coderwall.com/p/l1omyw/ruby-reading-parsing-and-forwarding-large-json-files-in-small-chunks-i-e-streaming

module PsUtilities
  class StreamParser < ::Oj::ScHandler
    def initialize(&block)
      @path = []
      @yield_record = block
      @count = nil
    end

    def run(enumerable_data_source)
      io = Piperator::IO.new(enumerable_data_source)
      Oj.sc_parse(self, io)
    end

    def hash_start
      update_path(:hash_start)
      {}
    end

    def hash_end
      update_path(:hash_end)
    end

    def hash_key(key)
      update_path(:hash_key, key)
      key
    end

    def hash_set(hash, key, value)
      return if @path == ['record']

      @count = value if @path == ['count']
      hash[key] = value
    end

    def array_start
      update_path(:array_start)
      []
    end

    def array_end
      update_path(:array_end)
    end

    def array_append(array, value)
      index = update_path(:array_append)
      if @path[..0] == ['record']
        @yield_record.call(value, index - 1, @count)
      else
        array << value
      end
    end

    private

    def update_path(method, key = nil)
      case method
      when :hash_start, :array_start
        @path << nil
      when :hash_end, :array_end
        @path.pop
      when :hash_key
        @path[@path.length - 1] = key
      when :array_append
        @path[@path.length - 1] = @path.last.to_i + 1
      end
    end
  end
end
