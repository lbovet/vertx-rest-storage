package li.chee.vertx.reststorage.lua;

import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class RedisDelLuaScriptTests extends AbstractLuaScriptTest {

    private final static String RESOURCE = "resource";
    private static final double MAX_EXPIRE_IN_MILLIS = 9999999999999d;

    @Test
    public void deleteResource2BranchesDeleteOnRootNode() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnForkNode() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", System.currentTimeMillis(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOneLevelAboveBranch() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test11"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test22"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test11:test22", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnOneResource() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1:test2");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test11"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), 9999999999999d).iterator().next(), equalTo("test22"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test11:test22", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnBothResources() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1:test2");
        evalScriptDel(":project:server:test:test11:test22");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteExpiredResourceWithMaxScoreAtMax() throws InterruptedException {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", "9999999999999");
        Thread.sleep(10);

        // ACT
        evalScriptDel(":project:server:test:test1:test2");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));

        String valueGet = (String) evalScriptGet(":project:server:test:test1:test2", getNowAsString());
        assertThat(valueGet, equalTo("notFound"));

    }

    @Test
    public void deleteExpiredResourceWithMaxScoreNow() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test1:test2", after);

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), 9999999999999d).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));

        String valueGet = (String) evalScriptGet(":project:server:test:test1:test2", getNowAsString());
        assertThat(valueGet, equalTo("notFound"));
    }

    @Test
    public void deleteExpiredResourceOfTwo() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000sec = String.valueOf((System.currentTimeMillis() + 1000000));
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", nowPlus1000sec);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test1:test2", after);

        // ASSERT
        String afterNow = String.valueOf(System.currentTimeMillis());
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", Double.valueOf(afterNow).doubleValue(), 9999999999999d).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", Double.valueOf(afterNow).doubleValue(), 9999999999999d).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", Double.valueOf(afterNow).doubleValue(), 9999999999999d).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", Double.valueOf(afterNow).doubleValue(), 9999999999999d).size(), equalTo(1));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(true));
    }

    @Test
    public void deleteNotExpiredResourceOfTwo() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000sec = String.valueOf((System.currentTimeMillis() + 1000000));
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", nowPlus1000sec);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test11:test22", after);

        // ASSERT
        assertThat(jedis.exists("rest-storage:collections:project"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server:test"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server:test:test11"), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptDel(final String resourceName) {
        String putScript = readScript("del.lua");
        return jedis.eval(putScript, new ArrayList() {
            {
                add(resourceName);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(prefixDeltaResources);
                add(prefixDeltaEtags);
                add(expirableSet);
                add("0");
                add("9999999999999");
            }
        }
                );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptDel(final String resourceName, final long maxscore) {
        String delScript = readScript("del.lua");
        return jedis.eval(delScript, new ArrayList() {
            {
                add(resourceName);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(prefixDeltaResources);
                add(prefixDeltaEtags);
                add(expirableSet);
                add(getNowAsString());
                add(String.valueOf(MAX_EXPIRE_IN_MILLIS));
            }
        }
                );
    }
}
