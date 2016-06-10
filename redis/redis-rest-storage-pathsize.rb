#!/usr/bin/ruby

############################################################################################
# Evalutes the memory usage of the subtree provided as path
#
# Attention: Be aware that LUA scripts to block all other redis operations. Do not use this
# script against a running (production) database!
#
# usage: ruby redis-rest-storage-pathsize.rb -t /rest/resources
# t is the path to evaluate
############################################################################################
require 'optparse'

options = {:target => nil, :host => 'localhost', :port => 6379, :resources => 'rest-storage:resources', :collections => 'rest-storage:collections'}

parser = OptionParser.new do|opts|
    opts.banner = "Usage: analyze_redis.rb [options]"
    opts.separator  ""
    opts.on('-t', '--target TARGET_PATH', 'The target path to analzye') do |path|
        options[:target] = path;
	end

    opts.on('-s', '--redis-host REDIS-HOST', 'The redis server host. Default is localhost') do |host|
		options[:host] = host;
	end
    
    opts.on('-p', '--redis-port REDIS-PORT', 'The redis server port. Default is 6379') do |port|
		options[:port] = port;
	end
    
    opts.on('-r', '--resources-prefix RESSOURCES-PREFIX', 'The key prefix for resources. Default is rest-storage:resources') do |resources|
        options[:resources] = resources;
	end    
    
    opts.on('-c', '--collections-prefix COLLECTIONS-PREFIX', 'The key prefix for collections. Default is rest-storage:collections') do |collections|
        options[:collections] = collections;
	end    

	opts.on('-h', '--help', 'Displays Help') do
		puts opts
		exit
	end
end

parser.parse!

if options[:target] == nil
    print 'Enter target path: '
    options[:target] = gets.chomp
end

def buildTargetKey(path)
    if !path.start_with?("/")
        path = "/".concat(path)
    end
    
    if path.end_with?("/")
        path = path[0...-1]
    end
    
    return path.gsub("/", ":")
end

targetKey = buildTargetKey(options[:target])
start = Time.now

puts '###################################################################'
puts ''
puts 'Starting analysis:'
puts '------------------'
puts 'target path: ' + options[:target]
puts 'redis host: ' + options[:host]
puts 'redis port: ' + String(Integer(options[:port]))
puts 'redis resource key: ' + options[:resources] + targetKey
puts ''
puts 'be patient this could take a while...'
puts ''
puts `redis-cli -h #{options[:host]} -p #{options[:port]} EVAL "$(cat analyze-pathsize.lua)" 1 #{targetKey} #{options[:resources]} #{options[:collections]}`
puts ''
puts 'script execution time: ' + String(Float(Time.now - start)) + ' seconds'
puts ''
puts '###################################################################'