class Fluent::ExecReducerOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('exec_reducer', self)

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d%H' # %Y%m%d%H%M

  config_param :command, :string
  config_param :in_keys, :string
  config_param :remove_prefix, :string, :default => nil
  config_param :out_keys, :string
  config_param :strip_output, :bool, :default => true
  config_param :add_prefix, :string, :default => nil
  config_param :tag, :string, :default => nil
  config_param :tag_key, :string, :default => nil
  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil
  config_param :localtime, :bool, :default => true

  def initialize
    super
    require 'time'
  end

  def configure(conf)
    super

    @localtime = if conf.has_key?('localtime')
                   conf['localtime']
                   true
                 else
                   false
                 end

    if !@tag && !@tag_key
      raise ConfigError, "'tag' or 'tag_key' option is required on exec_reducer output"
    end

    @in_keys = @in_keys.split(',')
    @out_keys = @out_keys.split(',')

    if @time_key
      if @time_format
        @timef = Fluent::TimeFormatter.new(@time_format, @localtime)
        @time_format_proc = @timef.method(:format)
        @time_parse_proc = Proc.new {|str| Time.strptime(str, @time_format).to_i }
      else
        @time_format_proc = Proc.new {|time| time.to_s }
        @time_parse_proc = Proc.new {|str| str.to_i }
      end
    end

    if @remove_prefix
      @removed_prefix_string = @remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
    if @add_prefix
      @added_prefix_string = @add_prefix + '.'
    end

    @procs = {} # time_slice_key => {:pid => pid, :thread => thread, :io => popen-object}
  end

  def start
    super
    $log.debug "out_exec_reducer start called!!!!"
    start_watch
  end

  def start_watch
    # for internal, or tests only
    @watcher = Thread.new(&method(:watch))
  end

  def watch
    while true
      sleep 0.5
      now_slice = @time_slicer.call(Engine.now.to_i - @time_slice_wait)
      $log.debug "watching slices, from now_slice #{now_slice}"
      slices = @procs.keys
      slices.each do |target_slice|
        next if target_slice >= now_slice
        $log.debug "closing expired slice #{target_slice}"
        proc = @procs[target_slice]
        proc[:io].close_write if proc[:io]
        proc[:thread].join #TODO set timeout
        proc.delete(:io)
        proc.delete(:thread)
        $log.debug "closed slice #{target_slice}"
        @procs.delete(target_slice)
      end
    end
  end

  def shutdown
    super

    $log.debug "out_exec_reducer shutdown called!!!!"
    return nil if @procs.keys.length < 1
    
    slices = @procs.keys
    slices.each do |s|
      if @procs[s][:io]
        io = @procs[s].delete(:io)
        io.close_write
      end
    end
    slices.each do |s|
      @procs[s][:thread].join(1)
    end

    @watcher.terminate if @watcher.alive?

    slices.each do |s|
      @procs[s][:thread].terminate
      @procs[s][:thread].join
    end
    nil
  end

  def format(tag, time, record)
    if @remove_prefix
      if tag == @remove_prefix or (tag[0,@removed_length] == @removed_prefix_string and tag.length > @removed_length)
        tag = tag[@removed_length..-1] || ''
      end
    end
    @in_keys.map{|key|
      case key
      when @time_key
        @time_format_proc.call(time)
      when @tag_key
        tag
      else
        record[key].to_s
      end
    }.join("\t") + "\n"
  end

  def run(io, pid, slice_time)
    while not io.eof?
      begin
        line = io.readline
        line.chomp!
        vals = line.split("\t")
        tag = @tag
        time = nil
        record = {}
        for i in 0...(@out_keys.length)
          key = @out_keys[i]
          val = vals[i]
          if key == @time_key
            time = @time_parse_proc.call(val)
          elsif key == @tag_key
            tag = if @add_prefix
                    @added_prefix_string + val
                  else
                    val
                  end
          else
            if @strip_output
              record[key] = val.strip
            else
              record[key] = val
            end
          end
        end
        if tag
          time ||= slice_time
          Fluent::Engine.emit(tag, time, record)
        end
      rescue
        $log.error "exec_reducer failed to emit", :error => $!, :line => line
        $log.error_backtrace $!.backtrace
      end
    end
    Process.waitpid(pid) rescue true # ignore 'No child process' and others
  rescue
    $log.error "bailouting"
    Process.kill(:TERM, pid) rescue true # ignore errors
    Process.kill(:KILL, pid) rescue true # ignore errors
    $log.error "process killed...."
    raise
  end

  def wait_thread(slice)
    # for test only
    $log.debug "waiting for slice #{slice}"
    proc = @procs[slice]
    $log.debug "process for slice #{slice}: " + proc.inspect
    if proc and proc[:thread]
      thread = proc[:thread]
      $log.debug "status for waiting thread #{thread.object_id}: #{thread.status}"
      thread.join
    end
  end

  def write(chunk)
    unless @procs[chunk.key]
      _io = IO.popen(@command, 'r+')
      _io.sync = true
      _pid = _io.pid
      slice_time = Time.strptime(chunk.key, @time_slice_format).to_i
      _thread = Thread.new(_io, _pid, slice_time, &method(:run))
      @procs[chunk.key] = {:thread => _thread, :io => _io}
    end
    io = @procs[chunk.key][:io]
    now_slice = @time_slicer.call(Fluent::Engine.now.to_i - @time_slice_wait).to_i
    begin
      io.write chunk.read
      if chunk.key.to_i < now_slice
        $log.debug "closing fd in write for expired time slice"
        io.close_write
        @procs[chunk.key].delete(:io)
        @procs[chunk.key][:thread].join #TODO set timeout
      end
    rescue
      $log.error "failed to write chunk"
      raise
    end
  end
end
