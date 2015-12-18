package li.chee.vertx.reststorage.lua;

import redis.clients.jedis.Jedis;

/**
 * Created by kammermannf on 07.06.2015.
 */
class JedisFactory {

    public static Jedis createJedis() {
        return new Jedis("localhost", 6379, 5000);
    }
}
