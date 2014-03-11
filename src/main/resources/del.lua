local sep = ":";
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local minscore = 0
local maxscore = 9999999999999

local function deleteChildren(path)
    local isCollection = redis.call('exists',collectionsPrefix..path)
    local isResource = redis.call('exists',resourcesPrefix..path)
	if isCollection == 1 then
		local members = redis.call('zrangebyscore',collectionsPrefix..path,minscore,maxscore)
	 	for key,value in pairs(members) do
	 		local pathToDelete = path..":"..value
	 		deleteChildren(pathToDelete)
	 		redis.call('del', collectionsPrefix..path)
	 	end
	elseif isResource == 1 then
		redis.call('del', resourcesPrefix..path)
	else
		redis.log(redis.LOG_WARNING, "can't delete resource from type: "..path)
	end
end

-- CHECK OCCURENCE AND DELETE ITSELF
local exists = 0
if redis.call('exists',resourcesPrefix..KEYS[1]) == 1 then
	exists = 1
elseif redis.call('exists',collectionsPrefix..KEYS[1]) == 1 then
	exists = 1
end
if exists == 0 then
   	return "notFound"
end

-- REMOVE THE CHILDREN
deleteChildren(KEYS[1])

-- REMOVE THE ORPHAN PARENTS
local path = KEYS[1]..sep
local nodes = {path:match((path:gsub("[^"..sep.."]*"..sep, "([^"..sep.."]*)"..sep)))}
local pathDepth=0
local pathState
local nodetable = {}
local pathtable = {}
for key,value in pairs(nodes) do
    if pathState == nil then
        pathState = value
    else
    	pathState = pathState..sep..value
   	end 
    pathtable[pathDepth] = pathState
    nodetable[pathDepth] = value
    pathDepth = pathDepth + 1
end

table.remove(pathtable,pathDepth)

redis.log(redis.LOG_NOTICE, "pathDepth: "..pathDepth)
local orphanParents = 1
if pathDepth > 1 and redis.call('zcount', collectionsPrefix..pathtable[pathDepth-2],minscore,maxscore) > 1 then
	orphanParents = 0
end

redis.log(redis.LOG_NOTICE, "orphanParents: "..orphanParents)

local directParent = 1
for pathDepthState = pathDepth, 2, -1 do
	redis.log(redis.LOG_NOTICE, pathtable[pathDepthState-2])
	redis.log(redis.LOG_NOTICE, nodetable[pathDepthState-1])
    if orphanParents == 1 then
		 if redis.call('zcount', collectionsPrefix..pathtable[pathDepthState-2],minscore,maxscore) > 1 then
		 	redis.call('zrem', collectionsPrefix..pathtable[pathDepthState-2], nodetable[pathDepthState-1])
		 	break
		 end
		 redis.log(redis.LOG_NOTICE, "del :"..collectionsPrefix..pathtable[pathDepthState-2])
		 redis.call('del', collectionsPrefix..pathtable[pathDepthState-2])
	end
	if directParent == 1 then
		redis.call('zrem', collectionsPrefix..pathtable[pathDepthState-2], nodetable[pathDepthState-1])
		directParent = 0
	end
end

return "deleted"
