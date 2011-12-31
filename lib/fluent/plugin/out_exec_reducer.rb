class Fluent::ExecReducerOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('exec_reducer', self)

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d' # %Y%m%d%H

  # config_param :hoge, :string, :default => 'hoge'

  def initialize
    super
    # require 'hogepos'
  end

  def configure(conf)
    super
    # @path = conf['path']
  end

  def start
    super
    # init
  end

  def shutdown
    super
    # destroy
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    records = []
    chunk.msgpack_each { |record|
      # records << record
    }
    # write records
  end
end
