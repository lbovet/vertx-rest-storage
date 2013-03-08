local s=redis.call('get',KEYS[1])
if s then
    s=cjson.decode(s)
    for k,v in pairs(cjson.decode(ARGV[1])) do 
        if v == cjson.null then s[k] = nil else	s[k] = v end        
    end
    s=cjson.encode(s)
else
    s=ARGV[1]
end
redis.call('set',KEYS[1],s)
