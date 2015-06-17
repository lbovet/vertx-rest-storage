local sep = ":"
local path = KEYS[1]
local parentPathElement
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local expirableSet = ARGV[3]
local timestamp = tonumber(ARGV[4])
local maxtime = tonumber(ARGV[5])
local offset = tonumber(ARGV[6])
local count = tonumber(ARGV[7])
local etag = ARGV[8]

local function not_empty(x)
    return (type(x) == "table") and (not x.err) and (#x ~= 0)
end

local function string_not_empty(s)
    return s ~= nil and s ~= ''
end

if redis.call('exists',resourcesPrefix..path) == 1 then
    local score = tonumber(redis.call('zscore',expirableSet,resourcesPrefix..path))
    if score ~= nil and score < timestamp then
        return "notFound"
    else
        local result = redis.call('hmget',resourcesPrefix..path,'resource','etag')
        if not_empty(result) then
            if string_not_empty(etag) then
                local etagStorage = result[2]
                if etagStorage == etag then
                    return "notModified"
                end
            end
            table.insert(result, 1, "TYPE_RESOURCE")
            return result
        else
            return "notFound"
        end
    end
elseif redis.call('exists',collectionsPrefix..path) == 1 then
    local members = {}
    if offset ~= nil and count ~= nil and offset > -1 then
        members = redis.call('zrangebyscore',collectionsPrefix..path, timestamp, maxtime,'limit',offset, count)
    else
        members = redis.call('zrangebyscore',collectionsPrefix..path, timestamp, maxtime)
    end
    local children = {}
    table.insert(children, 1, "TYPE_COLLECTION")
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