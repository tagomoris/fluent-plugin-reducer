require 'helper'

require 'helper'
# require 'time'

class ExecReducerOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    command wc -l
    in_keys time,k1
    out_keys k2
    time_key time
    tag reduced
    time_slice_wait 1s
    flush_interval 1s
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::ExecReducerOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver

    assert_equal '%Y%m%d%H', d.instance.time_slice_format
    assert_equal ['time', 'k1'], d.instance.in_keys
    assert_equal ['k2'], d.instance.out_keys
    assert_equal 'time', d.instance.time_key
    assert_equal "reduced", d.instance.tag
    assert_equal false, d.instance.localtime
    assert_equal true, d.instance.strip_output

    d = create_driver %[
      time_slice_format %Y%m%d%H%M
      command sort -u
      in_keys time,tag,k1
      remove_prefix before
      out_keys time,tag,k2
      add_prefix after
      tag_key tag
      time_key time
      time_format %Y%m%d%H%M
      localtime
    ]

    assert_equal '%Y%m%d%H%M', d.instance.time_slice_format
    assert_equal 'sort -u', d.instance.command
    assert_equal ['time', 'tag', 'k1'], d.instance.in_keys
    assert_equal 'before', d.instance.remove_prefix
    assert_equal ['time', 'tag', 'k2'], d.instance.out_keys
    assert_equal 'after', d.instance.add_prefix
    assert_equal 'tag', d.instance.tag_key
    assert_equal 'time', d.instance.time_key
    assert_equal '%Y%m%d%H%M', d.instance.time_format
    assert_equal true, d.instance.localtime
  end

  def test_format
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"k1"=>'number one'}, time)
    d.emit({"k1"=>'number two'}, time)
    d.expect_format time.to_s + %[\tnumber one\n]
    d.expect_format time.to_s + %[\tnumber two\n]
    # d.instance.start_watch
    d.run
    d.instance.wait_thread('2011010222')

    d = create_driver(%[
      time_slice_format %Y%m%d%H%M
      command sort -u
      in_keys time,tag,k1
      remove_prefix before
      out_keys time,tag,k2
      add_prefix after
      tag_key tag
      time_key time
      time_format %Y%m%d%H%M
      localtime
    ], 'before.testXXX')
    time = Time.parse("2011-01-02 13:14:15 JST").to_i
    d.emit({"k1"=>'value one'}, time)
    d.emit({"k1"=>'value two'}, time)
    d.expect_format %[201101021314\ttestXXX\tvalue one\n]
    d.expect_format %[201101021314\ttestXXX\tvalue two\n]
    # d.instance.start_watch
    d.run
    d.instance.wait_thread('201101021314')
  end

  def test_write
    d = create_driver
    time1 = Time.parse("2011-01-02 13:14:15 UTC").to_i
    timeslice_1 = Time.parse("2011-01-02 13:00:00 UTC").to_i
    d.emit({"k1"=>11}, time1)
    d.emit({"k1"=>21}, time1)
    d.emit({"k1"=>31}, time1)
    d.emit({"k1"=>41}, time1)
    d.run
    d.instance.wait_thread('2011010222')
    emits = d.emits
    assert_equal 1, emits.length
    assert_equal ['reduced', timeslice_1, {"k2"=>'4'}], emits[0]

    d = create_driver
    time1 = Time.parse("2011-01-02 13:14:15 UTC").to_i
    timeslice_1 = Time.parse("2011-01-02 13:00:00 UTC").to_i
    time2 = Time.parse("2011-01-02 14:15:16 UTC").to_i
    timeslice_2 = Time.parse("2011-01-02 14:00:00 UTC").to_i
    d.emit({"k1"=>31}, time1)
    d.emit({"k1"=>41}, time2)
    d.run
    d.instance.wait_thread('2011010222')
    d.instance.wait_thread('2011010223')
    emits = d.emits
    assert_equal 2, emits.length
    assert_equal ['reduced', timeslice_1, {"k2"=>'1'}], emits[0]
    assert_equal ['reduced', timeslice_2, {"k2"=>'1'}], emits[1]
  end

  def test_write_ontime
    d = create_driver
    now = Time.now
    time = now.to_i
    str = 'fooooooo baaaaarrrrrr'
    d.emit({"k1"=>str}, time)
    d.run
    slice = now.strftime(d.instance.time_slice_format)
    d.instance.wait_thread(slice)
    emits = d.emits
    assert_equal slice, Time.now.strftime(d.instance.time_slice_format)
    assert_equal 1, emits.length
    assert_equal ['reduced', Time.strptime(slice, d.instance.time_slice_format).to_i, {"k2"=>'1'}], emits[0]
  end
end
