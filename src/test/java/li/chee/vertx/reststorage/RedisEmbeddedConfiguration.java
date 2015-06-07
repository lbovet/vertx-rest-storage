package li.chee.vertx.reststorage;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;

public class RedisEmbeddedConfiguration {

    public final static RedisExecProvider customProvider = RedisExecProvider.defaultProvider()
            .override(OS.WINDOWS, Architecture.x86, "redis/redis-server.exe")
            .override(OS.WINDOWS, Architecture.x86_64, "redis/redis-server.exe");

    public final static RedisServer redisServer = RedisServer.builder()
            .redisExecProvider(customProvider)
            .port(6379)
            .build();

    public static boolean useExternalRedis() {
        String externalRedis = System.getenv("EXTERNAL_REDIS");
        return externalRedis != null;
    }
}