local sep = ":";
local path = KEYS[1]..sep
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local merge = ARGV[3]
local timestamp = tonumber(ARGV[4])
local resourceValue = ARGV[5]
local pathState
local collections = {}
local nodes = {path:match((path:gsub("[^"..sep.."]*"..sep, "([^"..sep.."]*)"..sep)))}
for key,value in pairs(nodes) do
    if pathState == nil then
    	pathState = value
    else
    	collections[pathState] = value
    	pathState = pathState..sep..value
   	end 
end
for key,value in pairs(collections) do
	local collectionKey = collectionsPrefix..key
    redis.call('zadd',collectionKey,timestamp,value)
end
if merge == "true" then
	local s = redis.call('get',resourcesPrefix..KEYS[1])
	if s then
	    s = cjson.decode(s)
	    for k,v in pairs(cjson.decode(resourceValue)) do 
	        if v == cjson.null then s[k] = nil else	s[k] = v end        
	    end
	    resourceValue = cjson.encode(s)
	end
end
redis.call('set',resourcesPrefix..KEYS[1],resourceValue) 
