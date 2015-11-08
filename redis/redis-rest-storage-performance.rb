#!/usr/bin/ruby

############################################################################################
# Evalutes the time spent in redis for a rest-storage call
# The execution time is collected over the redis slowlog command
# http://redis.io/commands/slowlog
# You have to consider, that the slowlog only has 128 entries max
#
# usage: ruby redis-rest-storage-performance.rb -u http://host:port/rest/resource -s 500
# u is the url to call / evaluate
# s is the number of calls / samples to execute
############################################################################################
require 'optparse'

############################################################################################
# Parse the redis slowlog and create an object of every entry
# Because we have no separator, we take the unique progressive identifier as identifier 
# for a new entry
# 
# slowlog: the slowlog of the actual run
# redis_calls: the redis_calls the executed runs
############################################################################################
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

############################################################################################
# Sets the limit to enter the slowlog for a command to 1 microsecond.
# Resets the slowlog
# Executes the rest-storage call
# Gets the slowlog
############################################################################################
def execute_rest_storage_and_read_slowlog(url)
	`redis-cli config set slowlog-log-slower-than 1`
	`redis-cli slowlog reset`
	`curl -s #{url} > /dev/null`
	slowlog = `redis-cli slowlog get 128`
	lines = slowlog.split(/\n+/)
	return lines
end

############################################################################################
# Parse the options
############################################################################################
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

############################################################################################
# Execute the calls
############################################################################################
redis_calls = Hash.new
samples.times do |i|
	lines = execute_rest_storage_and_read_slowlog(url)
	redis_calls = parse_slowlog(lines, redis_calls)
end

############################################################################################
# Collect the data of every call and calculate the average
############################################################################################
result = Hash.new
redis_calls.each do |key,redis_call|
	avg_time = redis_call[:time].inject{|sum,x| sum + x } / samples
	result[key]=avg_time
end
result.sort_by {|key,time| time}.each do |key,value|
	puts "time: " + value.to_s + " key: " + key
end

############################################################################################
# Calculate the time spent in redis calls and the time spent in lua
############################################################################################
evalshaPlusRedis = result.clone.delete_if{|key,value| !key.match(/^evalsha.*/)}.values.inject{|sum,a| sum + a}
onlyRedis = result.clone.delete_if{|key,value| key.match(/^evalsha.*/)}.values.inject{|sum,a| sum + a}
puts "time in redis calls: " + onlyRedis.to_s
puts "time in lua: " + (evalshaPlusRedis-onlyRedis).to_s
