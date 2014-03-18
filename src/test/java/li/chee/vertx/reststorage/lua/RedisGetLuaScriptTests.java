package li.chee.vertx.reststorage.lua;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class RedisGetLuaScriptTests {

    Jedis jedis = null;

    private final static String prefixResources = "rest-storage:resources";
    private final static String prefixCollections = "rest-storage:collections";
    private final static String expirableSet = "rest-storage:expirable";

    @Before
    public void connect() {
        jedis = new Jedis("localhost");
    }

    @After
    public void disconnnect() {
        jedis.flushAll();
        jedis.close();
    }

    @Test
    public void getResourcePathDepthIs3() {

        // ARRANGE
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        String value = (String) evalScriptGet(":nemo:server:test:test1:test2");

        // ASSERT
        assertThat("{\"content\": \"test/test1/test2\"}", equalTo(value));
    }

    @Test
    public void getCollectionPathDepthIs3ChildIsResource() {

        // ARRANGE
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) evalScriptGet(":nemo:server:test:test1");

        // ASSERT
        assertThat(values.get(0), equalTo("test2"));
    }

    @Test
    public void getCollectionPathDepthIs3HasOtherCollection() {

        // ARRANGE
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) evalScriptGet(":nemo:server:test");

        // ASSERT
        assertThat(values.get(0), equalTo("test1:"));
    }

    // EXPIRATION

    @Test
    public void getResourcePathDepthIs3ParentOfResourceIsExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        List<String> values = (List<String>) evalScriptGet(":nemo:server:test:test1");

        // ASSERT
        assertTrue(values.isEmpty());
    }

    @Test
    public void getResourcePathDepthIs3ResourceIsExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        String value = (String) evalScriptGet(":nemo:server:test:test1:test2", timestamp);

        // ASSERT
        assertThat(value, equalTo("notFound"));
    }

    @Test
    public void getResourcePathDepthIs3ParentOfResourceHasUpdatedExpiration() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String in1min = String.valueOf(System.currentTimeMillis() + (1000 * 60));
        evalScriptPut(":nemo:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":nemo:server:test:test1:test22", "{\"content\": \"test/test1/test22\"}", in1min);
        Thread.sleep(10);

        // ACT
        List<String> valuesTest1 = (List<String>) evalScriptGet(":nemo:server:test:test1");
        String valueTest2 = (String) evalScriptGet(":nemo:server:test:test1:test2");
        String valueTest22 = (String) evalScriptGet(":nemo:server:test:test1:test22");

        // ASSERT
        assertThat(valuesTest1.size(), equalTo(1));
        assertThat(valuesTest1.iterator().next(), equalTo("test22"));
        assertThat(valueTest2, equalTo("notFound"));
        assertThat(valueTest22, equalTo("{\"content\": \"test/test1/test22\"}"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private String evalScriptPut(final String resourceName1, final String resourceValue1) {
        String putScript = readScript("put.lua");
        return (String) jedis.eval(putScript, new ArrayList() {
            {
                add(resourceName1);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(expirableSet);
                add("false");
                add("9999999999999");
                add(resourceValue1);
            }
        }
                );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private void evalScriptPut(final String resourceName1, final String resourceValue1, final String expire) {
        String putScript = readScript("put.lua");
        jedis.eval(putScript, new ArrayList() {
            {
                add(resourceName1);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(expirableSet);
                add("false");
                add(expire);
                add(resourceValue1);
            }
        }
                );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptGet(final String resourceName1) {
        String getScript = readScript("get.lua");
        return jedis.eval(getScript, new ArrayList() {
            {
                add(resourceName1);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(expirableSet);
                add(String.valueOf(System.currentTimeMillis()));
                add("9999999999999");
            }
        }
                );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptGet(final String resourceName1, final String timestamp) {
        String getScript = readScript("get.lua");
        return jedis.eval(getScript, new ArrayList() {
            {
                add(resourceName1);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(expirableSet);
                add(timestamp);
                add("9999999999999");
            }
        }
                );
    }

    private String readScript(String scriptFileName) {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(scriptFileName)));
        StringBuilder sb;
        try {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
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
