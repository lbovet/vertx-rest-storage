local sep = ":"
local path = KEYS[1]
local parentPathElement
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local expirableSet = ARGV[3]
local timestamp = tonumber(ARGV[4])
local maxtime = tonumber(ARGV[5])

if redis.call('exists',resourcesPrefix..path) == 1 then
	local score = tonumber(redis.call('zscore',expirableSet,resourcesPrefix..path))
	if score == nil or score < timestamp then
		return "notFound"
	else
		return redis.call('get',resourcesPrefix..path)	
	end
elseif redis.call('exists',collectionsPrefix..path) == 1 then
	local members = redis.call('zrangebyscore',collectionsPrefix..path, timestamp, maxtime)
	local children = {}
	for key,value in pairs(members) do
		local childPath = collectionsPrefix..path..sep..value
	 	if redis.call('type', childPath)["ok"] == "zset" then
	 		table.insert(children, value..sep)
	 	else
	 		table.insert(children, value)
	 	end
 	end
 	return children
else
	return "notFound"
end	