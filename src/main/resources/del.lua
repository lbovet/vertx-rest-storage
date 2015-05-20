local sep = ":";
local toDelete = KEYS[1]
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local deltaResourcesPrefix = ARGV[3]
local deltaEtagsPrefix = ARGV[4]
local expirableSet = ARGV[5]
local minscore = tonumber(ARGV[6])
local maxscore = tonumber(ARGV[7])

local function deleteChildrenAndItself(path)
    if redis.call('exists',resourcesPrefix..path) == 1 then
      redis.log(redis.LOG_NOTICE, "del: "..resourcesPrefix..path)
      redis.call('zrem', expirableSet, resourcesPrefix..path)
      redis.call('del', resourcesPrefix..path)
      redis.call('del', deltaResourcesPrefix..path)
      redis.call('del', deltaEtagsPrefix..path)
    elseif redis.call('exists',collectionsPrefix..path) == 1 then
      local members = redis.call('zrangebyscore',collectionsPrefix..path,minscore,maxscore)
      for key,value in pairs(members) do
        local pathToDelete = path..":"..value
        deleteChildrenAndItself(pathToDelete)
        redis.call('del', collectionsPrefix..path)
      end
    else
      redis.log(redis.LOG_WARNING, "can't delete resource from type: "..path)
    end
end

local scriptState = "notFound"

-- CHECK OCCURENCE
if redis.call('exists',resourcesPrefix..toDelete) == 1 or redis.call('exists',collectionsPrefix..toDelete) == 1 then 

  local score = tonumber(redis.call('zscore',expirableSet,resourcesPrefix..toDelete))  
  local expired = 0
  if score ~= nil and minscore > score then
    redis.log(redis.LOG_NOTICE, "expired: "..resourcesPrefix..toDelete)
    expired = 1
  end
  
  if expired == 0 then
  
    -- REMOVE THE CHILDREN
    deleteChildrenAndItself(toDelete)
    
    if redis.call('zcount', collectionsPrefix..toDelete,minscore,maxscore) == 0 then
      
      -- REMOVE THE ORPHAN PARENTS
      local path = toDelete..sep
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
          redis.log(redis.LOG_NOTICE, "add path: "..pathDepth.." "..pathState)
          pathtable[pathDepth] = pathState
          nodetable[pathDepth] = value
          pathDepth = pathDepth + 1
      end
      
      table.remove(pathtable,pathDepth)
      
      local orphanParents = 1
      local parentCount = redis.call('zcount', collectionsPrefix..pathtable[pathDepth-2],minscore,maxscore)
      redis.log(redis.LOG_NOTICE, "parentCount: "..parentCount)
      redis.log(redis.LOG_NOTICE, "pathDepth: "..pathDepth)
      if pathDepth > 1 and parentCount > 1 then
        orphanParents = 0
      end
      
      redis.log(redis.LOG_NOTICE, "orphanParents: "..orphanParents)
      
      local directParent = 1
      local stopDel = 0
      for pathDepthState = pathDepth, 2, -1 do
        redis.log(redis.LOG_NOTICE, "pathState: "..pathtable[pathDepthState-2].." "..pathDepthState)
        if orphanParents == 1 and stopDel == 0 then
            if redis.call('zcount', collectionsPrefix..pathtable[pathDepthState-2],0,maxscore) > 1 then
              stopDel = 1
            end
            redis.log(redis.LOG_NOTICE, "zrem: "..collectionsPrefix..pathtable[pathDepthState-2].." "..nodetable[pathDepthState-1])
            redis.call('zrem', collectionsPrefix..pathtable[pathDepthState-2], nodetable[pathDepthState-1])
        end
        if directParent == 1 then
          redis.log(redis.LOG_NOTICE, "remove direct parent")
          redis.log(redis.LOG_NOTICE, "zrem: "..collectionsPrefix..pathtable[pathDepth-2].." "..nodetable[pathDepthState-1])
          redis.call('zrem', collectionsPrefix..pathtable[pathDepthState-2], nodetable[pathDepthState-1])
          directParent = 0
        end
      end
    end
    
    scriptState = "deleted"
  
  end
  
end

return scriptState