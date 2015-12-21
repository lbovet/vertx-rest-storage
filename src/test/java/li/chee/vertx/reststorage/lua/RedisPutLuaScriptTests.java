package li.chee.vertx.reststorage.lua;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RedisPutLuaScriptTests extends AbstractLuaScriptTest {

    private final static String RESOURCE = "resource";
    private final static String ETAG = "etag";

    @Test
    public void putResourcePathDepthIs3() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ASSERT
        assertThat("server", equalTo(jedis.zrangeByScore("rest-storage:collections:project", 0d, 9999999999999d).iterator().next()));
        assertThat("test", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server", 0d, 9999999999999d).iterator().next()));
        assertThat("test1", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server:test", 0d, 9999999999999d).iterator().next()));
        assertThat("test2", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", 0d, 9999999999999d).iterator().next()));
        assertThat("{\"content\": \"test/test1/test2\"}", equalTo(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE)));
    }

    @Test
    public void putResourcePathDepthIs3WithSiblings() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11", "{\"content\": \"test/test11\"}");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", 0d, 9999999999999d).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", 0d, 9999999999999d).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", 0d, 9999999999999d).iterator().next(), equalTo("test1"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", 0d, 9999999999999d).iterator().next(), equalTo("test2"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void putResourcePathDepthIs3WithSiblingsFolderAndDocument() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", 0d, 9999999999999d).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", 0d, 9999999999999d).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", 0d, 9999999999999d).iterator().next(), equalTo("test1"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", 0d, 9999999999999d).iterator().next(), equalTo("test2"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void putResourceMergeOnEmpty() {

        // ACT
        evalScriptPutMerge(":project:server:test:test1:test2", "{\"content\": \"test_test1_test3\"}");

        // ASSERT
        String result = jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE);
        JsonObject obj = new JsonObject(result);
        assertThat(obj.getString("content"), equalTo("test_test1_test3"));
    }

    @Test
    public void putResourceMergeOnExisting() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test_test1_test2\"}");
        evalScriptPutMerge(":project:server:test:test1:test2", "{\"content\": \"test_test1_test3\"}");

        // ASSERT
        String result = jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE);
        JsonObject obj = new JsonObject(result);
        assertThat(obj.getString("content"), equalTo("test_test1_test3"));
    }

    @Test
    public void putResourceWithProvidedEtagValue() {

        // ACT
        String originalContent = "{\"content\": \"originalContent\"}";
        String value = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, "myFancyEtagValue");
        String modifiedContent = "{\"content\": \"modifiedContent\"}";
        String value2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, "myFancyEtagValue");

        // ASSERT
        assertThat(value, is(not(equalTo("notModified"))));
        assertThat(value2, is(equalTo("notModified")));

        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo("myFancyEtagValue"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));
    }

    @Test
    public void putResourceWithNotProvidedEtagValue() {

        // ACT
        String originalContent = "{\"content\": \"originalContent\"}";
        String value = evalScriptPut(":project:server:test:test1:test2", originalContent);

        // ASSERT
        assertThat(value, is(not(equalTo("notModified"))));
        String etagValue = jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG);
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));

        // ACT
        String modifiedContent = "{\"content\": \"modifiedContent\"}";
        String value2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent);

        // ASSERT
        assertThat(value2, is(not(equalTo("notModified"))));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(modifiedContent));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), is(not(equalTo(etagValue))));
    }

    @Test
    public void tryToPutResourceOnCollectionPathOneLevelBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest2));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test2"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionPathTwoLevelsBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server:test", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest2));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test1:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionPathThreeLevelsBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest2));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionFirstLevel() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest2));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("server:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutCollectionOnResourceOneLevelAbovePath() {

        // ARRANGE
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        String valueTest2 = (String) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest1));
        assertThat(resultPutTest2, equalTo("existingResource rest-storage:resources:project:server:test:test1"));
        assertThat(valueTest1.get(1), equalTo("{\"content\": \"test/test1\"}"));
        assertThat(valueTest2, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceTwoLevelsAbovePath() {

        // ARRANGE
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");
        String resultPutTest3 = evalScriptPut(":project:server:test:test1:test2:test3", "{\"content\": \"test/test1/test2/test3\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        String valueTest3 = (String) evalScriptGet(":project:server:test:test1:test2:test3");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutTest1));
        assertThat(resultPutTest3, equalTo("existingResource rest-storage:resources:project:server:test:test1"));
        assertThat(valueTest1.get(1), equalTo("{\"content\": \"test/test1\"}"));
        assertThat(valueTest3, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceOneLevelOneLevelAbovePath() {

        // ARRANGE
        String resultPutNemo = evalScriptPut(":project", "{\"content\": \"nemo\"}");
        String resultPutNemoServer = evalScriptPut(":project:server", "{\"content\": \"nemo/server\"}");

        // ACT
        List<String> valueNemo = (List<String>) evalScriptGet(":project");
        String valueServer = (String) evalScriptGet(":project:server");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutNemo));
        assertThat(resultPutNemoServer, equalTo("existingResource rest-storage:resources:project"));
        assertThat(valueNemo.get(1), equalTo("{\"content\": \"nemo\"}"));
        assertThat(valueServer, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceFourLevelOneLevelAbovePath() {

        // ARRANGE
        String resultPutNemo = evalScriptPut(":nemo:server:tests:crush:test1", "{\"content\": \"nemo\"}");
        String resultPutNemoServer = evalScriptPut(":nemo:server:tests:crush:test1:test2:test3", "{\"content\": \"nemo/server\"}");

        // ACT
        List<String> valueNemo = (List<String>) evalScriptGet(":nemo:server:tests:crush:test1");
        String valueServer = (String) evalScriptGet(":nemo:server:tests:crush:test1:test2:test3");

        // ASSERT
        assertTrue(StringUtils.isEmpty(resultPutNemo));
        assertThat(resultPutNemoServer, equalTo("existingResource rest-storage:resources:nemo:server:tests:crush:test1"));
        assertThat(valueNemo.get(1), equalTo("{\"content\": \"nemo\"}"));
        assertThat(valueServer, equalTo("notFound"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptPutMerge(final String resourceName1, final String resourceValue1) {
        String putScript = readScript("put.lua");
        return jedis.eval(putScript, new ArrayList() {
                    {
                        add(resourceName1);
                    }
                }, new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(expirableSet);
                        add("true");
                        add("9999999999999");
                        add("9999999999999");
                        add(resourceValue1);
                        add(UUID.randomUUID().toString());
                    }
                }
        );
    }
}
