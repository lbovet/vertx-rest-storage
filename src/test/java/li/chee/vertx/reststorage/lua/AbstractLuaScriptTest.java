/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package li.chee.vertx.reststorage.lua;

import li.chee.vertx.reststorage.RedisEmbeddedConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class AbstractLuaScriptTest {

    final static String prefixResources = "rest-storage:resources";
    final static String prefixCollections = "rest-storage:collections";
    final static String expirableSet = "rest-storage:expirable";
    final static String prefixDeltaResources = "delta:resources";
    final static String prefixDeltaEtags = "delta:etags";

    Jedis jedis = null;

//    @BeforeClass
    public static void config() {
        if(!useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.start();
        }
    }

//    @AfterClass
    public static void stopRedis() {
        if(!useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.stop();
        }
    }

    @Before
    public void connect() {
        jedis = JedisFactory.createJedis();
    }

    @After
    public void disconnnect() {
        jedis.flushAll();
        jedis.close();
    }

    protected static boolean useExternalRedis() {
        String externalRedis = System.getenv("EXTERNAL_REDIS");
        return externalRedis != null;
    }

    protected double getNowAsDouble() {
        return Double.valueOf(System.currentTimeMillis()).doubleValue();
    }

    protected String getNowAsString() { return String.valueOf(System.currentTimeMillis()); }

    protected String readScript(String scriptFileName) {
        return readScript(scriptFileName, false);
    }

    protected String readScript(String scriptFileName, boolean stripLogNotice) {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(scriptFileName)));
        StringBuilder sb;
        try {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                if (stripLogNotice && line.contains("redis.LOG_NOTICE,")) {
                    continue;
                }
                sb.append(line + "\n");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        return sb.toString();
    }
}
