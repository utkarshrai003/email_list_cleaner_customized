#!/usr/bin/env ruby
require "rubygems"
require "bundler/setup"
require "yaml"
require "csv"
require "fileutils"
require "singleton"
require "pg"
require "benchmark"

Bundler.require(:default)

Dir.glob("lib/**/*.rb") {|f| require_relative f}

# Cleans list of emails (_list.csv) by looking for gibberish,
# de-duping, and connecting to SMTP servers.
class EmailListCleaner
  include Singleton

  # Redis namespace & keys
  # Operates out of "db 1"
  R_DEFAULT_DB = 1
  R_NAMESPACE  = "email_cleaner"
  R_SET_TODO   = "unverified"
  R_SET_GOOD   = "good"
  R_SET_BAD    = "bad"

  # --------------------------------------------------------

  # Database config.
  DB_HOST = 'csg.cyruxuioaadm.us-east-1.rds.amazonaws.com'
  DB_NAME = 'skydata'
  DB_USERNAME = 'csgdb'
  DB_PASSWORD = '0nmgjxrN'

  # ruby-progressbar format
  # https://github.com/jfelchner/ruby-progressbar/wiki/Formatting
  PROGRESS_FORMAT = "%t [%c/%C] %w"
  EXP_MICROSOFT = /.*\@(passport|hotmail|msn|live|outlook)\..*/i

  attr_reader :r_named, :config, :proxy_list, :pg

  def initialize
    @config = YAML::load_file("config.yml")
    @sleep_time = @config["sleep_time"].to_i
    config_redis
    config_email_verifier
    config_proxy_list
    establish_db_connection
  end

  # If proxy_addresses defined in proxylist.csv or config.yml, this provides
  # random proxy in that list to Net::SMTPs method that fetches a
  # TCPConnection.
  def random_proxy
    return nil unless num_proxies > 0
    return @proxy_list.sample
  end
  # Similarly, this provides round-robin proxy access
  def next_proxy
    return nil unless num_proxies > 0
    return @proxy_list[next_proxy_counter]
  end
  def next_proxy_counter
    @next_proxy_counter ||= 0
    max = num_proxies-1
    if @next_proxy_counter >= max
      @next_proxy_counter = 0
    else
      @next_proxy_counter += 1
    end
    return @next_proxy_counter 
  end

  def run
    load_csv_into_redis_set
    enum_and_verify
    dump_csv_files
    print_stats
  end

  # CSV expected to have "Name", "Email address" in each row
  # Optionally filters only emails that match regexp
  def load_csv_into_redis_set(regexp=nil)
    db_record_array = fetch_records
    @pg = ProgressBar.create(
      title: "Load into Redis",
      format: PROGRESS_FORMAT,
      total: db_record_array.length
    )

    # reset key
    @r_named.del(R_SET_TODO)
    db_record_array.each do |row|
      email = row[1]

      next unless email =~ regexp if regexp
      @r_named.sadd(R_SET_TODO, {id: row[0], email: row[1]}.to_json)
      @pg.increment
    end
    return @r_named.scard(R_SET_TODO)
  end

  def reset_redis_sets
    @r_named.del(R_SET_TODO)
    @r_named.del(R_SET_GOOD)
    @r_named.del(R_SET_BAD)
  end

  # Writes CSV files based on our current redis sets.
  def dump_csv_files
    FileUtils.mkdir_p('tmp')
    write_csv_file(R_SET_TODO, "tmp/_list_todo.csv")
    write_csv_file(R_SET_GOOD, "tmp/_list_good.csv")
    write_csv_file(R_SET_BAD,  "tmp/_list_bad.csv")
    puts "CSV files written to 'tmp' directory."
  end

  def write_csv_file(redis_key, file_name)
    email_arr = @r_named.smembers(redis_key) 
    File.open(file_name, "w") do |f|
      f << email_arr.join("\n")
    end
  end

  # Loops through all addresses and verifies.
  # The 'meat' of this program.
  #
  # (Creates 1 thread per proxy for speed)
  def enum_and_verify
    puts "Verifying..."
    @pg = ProgressBar.create(
      title: "Verifying",
      format: PROGRESS_FORMAT,
      total: @r_named.scard(R_SET_TODO)
    )
    @mutex = Mutex.new
    threads = []
    num_threads = num_proxies > 0 ? num_proxies : 1
    (1..num_threads).each do |i|
      threads << Thread.new { verify_until_done }
    end
    threads.each { |t| t.join }
  rescue SystemExit, Interrupt
    puts "Caught CTRL-C...stopping!"
    Thread.list.each { |t| Thread.kill(t) }
    return
  end

  def verify_until_done
    email = nil 
    while email = @r_named.spop(R_SET_TODO) do
      row = JSON.parse(email)
      sleep @sleep_time
      verify_email(row)
      @pg.increment
    end
  end

  def verify_email(row)
    id = row['id']
    email = row['email']
    @pg.log "\n= #{email}"
    success = false
    begin
      puts "****** Trying for mail checking server connection ********"
      time = Benchmark.measure do
        success = EmailVerifier.check(email)
      end
      puts "~~~~~~~~~~~~~~~~~~~~~~~~~~~~ > #{time}"
    rescue => e
      puts "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
      @pg.log "  (!) #{e.message}"
    end

    success ? update_mx_valid(id, false) : update_mx_valid(id, true)
  end

  def print_stats
    puts "REMAINING: #{@r_named.scard(R_SET_TODO)}"
    puts "GOOD: #{@r_named.scard(R_SET_GOOD)}"
    puts "BAD: #{@r_named.scard(R_SET_BAD)}"
  end

  # ===========================================================================
  private

  def config_redis
    r_config = {db: R_DEFAULT_DB}
    unless @config["redis_password"].to_s.empty?
      r_config["password"] = @config["redis_password"]
    end
    r_conn = Redis.new(db: "redis-14326.c11.us-east-1-3.ec2.cloud.redislabs.com:14326")
    # r_conn = Redis.new(r_config)
    @r_named = Redis::Namespace.new(R_NAMESPACE, redis: r_conn)
  end

  def config_email_verifier    
    EmailVerifier.config do |c|
      c.verifier_email = @config["from_email_address"]
    end
  end

  def config_proxy_list
    @proxy_list = @config["proxy_addresses"] || []
  end

  def num_proxies
    @proxy_list.length
  end

  def establish_db_connection
    @conn = PG::Connection.new(DB_HOST, nil, nil, nil, DB_NAME, DB_USERNAME, DB_PASSWORD)
  end

  def fetch_records
    @conn.exec('Select id, email from people limit 5').values
  end

  def update_mx_valid(id, mx_valid)
    time = Benchmark.measure do
      query = "UPDATE people SET mx_valid = #{mx_valid} WHERE id = #{id}";
      @conn.exec(query);
    end

    puts "----------------- >> #{time}"
  end
end

# For quick irb reference
ELC = EmailListCleaner.instance