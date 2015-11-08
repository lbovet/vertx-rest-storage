#!/usr/bin/ruby

require 'optparse'

def parse_slowlog(slowlog, redis_calls)
	first_id = slowlog.length-5
	start_id = slowlog[first_id].to_i

	i = slowlog.length-1
	next_id = start_id
	redis_call_str = Array.new
	redis_calls_str = Array.new
	while i >=0 do
		next_line = slowlog[i].strip
		redis_call_str << next_line
		if next_line == next_id.to_s
			next_id +=1
			redis_calls_str << redis_call_str
			redis_call_str = Array.new
		end
		i -= 1
	end

	redis_calls_str.each do |redis_call_str|

		time = redis_call_str[redis_call_str.length-3].to_i
		command = redis_call_str[redis_call_str.length-4]

		key = ""
		if(command == "evalsha")
			key = redis_call_str[redis_call_str.length-7]
		else
			key = redis_call_str[redis_call_str.length-5]
		end

		if(!key.start_with?("rest-storage") && !key.start_with?(":nemo"))
			next
		end

		command_key = command+"_"+key

		if(redis_calls.has_key?(command_key))
			redis_calls[command_key][:time] << time
		else
			redis_call = {:key => key, :time => [time], :command => command}
			redis_calls[command_key] = redis_call
		end

	end

	return redis_calls
end

def execute_rest_storage_and_read_slowlog(url)
	`redis-cli config set slowlog-log-slower-than 1`
	`redis-cli slowlog reset`
	`curl -s #{url} > /dev/null`
	#http://v00055:7012/nemo/server/tests/expand/10k/?expand=1
	slowlog = `redis-cli slowlog get 128`
	lines = slowlog.split(/\n+/)
	#lines = File.readlines('slowlog')
	return lines
end

options = {}

optparse = OptionParser.new do |opts|
	opts.banner = "test the performance of a rest-storage call"

	opts.on("-u", "--url URL", "the url to call") do |url|
		options[:url] =  url
	end

	opts.on("-s", "--samples SAMPLES", "the number of samples to take") do |samples|
		options[:samples] =  samples
	end

end

optparse.parse!

samples = options[:samples].to_i
url = options[:url]

redis_calls = Hash.new

samples.times do |i|
	lines = execute_rest_storage_and_read_slowlog(url)
	redis_calls = parse_slowlog(lines, redis_calls)
end

result = Hash.new
redis_calls.each do |key,redis_call|
	avg_time = redis_call[:time].inject{|sum,x| sum + x } / samples
	result[key]=avg_time
end
result.sort_by {|key,time| time}.each do |key,value|
	puts "time: " + value.to_s + " key: " + key
end

evalshaPlusRedis = result.clone.delete_if{|key,value| !key.match(/^evalsha.*/)}.values.inject{|sum,a| sum + a}
onlyRedis = result.clone.delete_if{|key,value| key.match(/^evalsha.*/)}.values.inject{|sum,a| sum + a}
puts "time in redis calls: " + onlyRedis.to_s
puts "time in lua: " + (evalshaPlusRedis-onlyRedis).to_s
