package li.chee.vertx.reststorage.lua;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class RedisGetLuaScriptTests extends AbstractLuaScriptTest {

    private final static String TYPE_COLLECTION = "TYPE_COLLECTION";
    private final static String TYPE_RESOURCE = "TYPE_RESOURCE";

    @Test
    public void getResourcePathDepthIs3() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        List<String> value = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(TYPE_RESOURCE, equalTo(value.get(0)));
        assertThat("{\"content\": \"test/test1/test2\"}", equalTo(value.get(1)));
    }

    @Test
    public void getResourceWithEtag() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(values.get(0), equalTo(TYPE_RESOURCE));
        assertThat(values.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
        assertThat(values.get(2), notNullValue());

        String etag = values.get(2);

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(values2.get(2), equalTo(etag));

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2x\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values3 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(values3.get(2), not(equalTo(etag)));
    }


    @Test
    public void getCollectionPathDepthIs3ChildIsResource() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) evalScriptGet(":project:server:test:test1");

        // ASSERT
        assertThat(values.get(0), equalTo(TYPE_COLLECTION));
        assertThat(values.get(1), equalTo("test2"));
    }

    @Test
    public void getCollectionPathDepthIs3HasOtherCollection() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) evalScriptGet(":project:server:test");

        // ASSERT
        assertThat(values.get(0), equalTo(TYPE_COLLECTION));
        assertThat(values.get(1), equalTo("test1:"));
    }

    // EXPIRATION

    @Test
    public void getResourcePathDepthIs3ParentOfResourceIsExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        List<String> values = (List<String>) evalScriptGet(":project:server:test:test1");

        // ASSERT
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0), equalTo(TYPE_COLLECTION));
    }

    @Test
    public void getResourcePathDepthIs3ResourceIsExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        String value = (String) evalScriptGet(":project:server:test:test1:test2", timestamp);

        // ASSERT
        assertThat(value, equalTo("notFound"));
    }

    @Test
    public void getResourcePathDepthIs3ParentOfResourceHasUpdatedExpiration() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String in1min = String.valueOf(System.currentTimeMillis() + (1000 * 60));
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":project:server:test:test1:test22", "{\"content\": \"test/test1/test22\"}", in1min);
        Thread.sleep(10);

        // ACT
        List<String> valuesTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        String valueTest2 = (String) evalScriptGet(":project:server:test:test1:test2");
        List<String> valueTest22 = (List<String>) evalScriptGet(":project:server:test:test1:test22");

        // ASSERT
        assertThat(valuesTest1.size(), equalTo(2));
        assertThat(valuesTest1.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest1.get(1), equalTo("test22"));
        assertThat(valueTest2, equalTo("notFound"));
        assertThat(valueTest22.get(0), equalTo(TYPE_RESOURCE));
        assertThat(valueTest22.get(1), equalTo("{\"content\": \"test/test1/test22\"}"));
    }
    
    @Test
    public void getCollectionInvalidOffsetCount() throws InterruptedException {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test1:test3", "{\"content\": \"test/test1/test3\"}");
        evalScriptPut(":project:server:test:test1:test4", "{\"content\": \"test/test1/test4\"}");
        evalScriptPut(":project:server:test:test1:test5", "{\"content\": \"test/test1/test5\"}");

        // ACT
        List<String> valuesTest1 = (List<String>) evalScriptGet(":project:server:test:test1", "bla", "blo");

        // ASSERT
        assertThat(valuesTest1.size(), equalTo(5));
        assertThat(valuesTest1.get(0), equalTo(TYPE_COLLECTION));
    }
    
    @Test
    public void getCollectionOffsetCountInsideBoundaries() throws InterruptedException {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test1:test3", "{\"content\": \"test/test1/test3\"}");
        evalScriptPut(":project:server:test:test1:test4", "{\"content\": \"test/test1/test4\"}");
        evalScriptPut(":project:server:test:test1:test5", "{\"content\": \"test/test1/test5\"}");

        // ACT
        List<String> valuesTest1 = (List<String>) evalScriptGet(":project:server:test:test1", "1", "2");

        // ASSERT
        assertThat(valuesTest1.size(), equalTo(3));
        assertThat(valuesTest1.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest1.get(1), equalTo("test3"));
        assertThat(valuesTest1.get(2), equalTo("test4"));
        
        // ACT
        List<String> valuesTest2 = (List<String>) evalScriptGet(":project:server:test:test1", "0", "4");

        // ASSERT
        assertThat(valuesTest2.size(), equalTo(5));
        assertThat(valuesTest2.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest2.get(1), equalTo("test2"));
        assertThat(valuesTest2.get(2), equalTo("test3"));
        assertThat(valuesTest2.get(3), equalTo("test4"));
        assertThat(valuesTest2.get(4), equalTo("test5"));
        
        // ACT
        List<String> valuesTest3 = (List<String>) evalScriptGet(":project:server:test:test1", "0", "-1");

        // ASSERT
        assertThat(valuesTest3.size(), equalTo(5));
        assertThat(valuesTest3.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest3.get(1), equalTo("test2"));
        assertThat(valuesTest3.get(2), equalTo("test3"));
        assertThat(valuesTest3.get(3), equalTo("test4"));
        assertThat(valuesTest3.get(4), equalTo("test5"));
    }
    
    @Test
    public void getCollectionOffsetCountOutsideBoundaries() throws InterruptedException {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test1:test3", "{\"content\": \"test/test1/test3\"}");
        evalScriptPut(":project:server:test:test1:test4", "{\"content\": \"test/test1/test4\"}");
        evalScriptPut(":project:server:test:test1:test5", "{\"content\": \"test/test1/test5\"}");

        // ACT
        List<String> valuesTest1 = (List<String>) evalScriptGet(":project:server:test:test1", "-1", "2");

        // ASSERT
        assertThat(valuesTest1.size(), equalTo(5));
        assertThat(valuesTest1.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest1.get(1), equalTo("test2"));
        assertThat(valuesTest1.get(2), equalTo("test3"));
        assertThat(valuesTest1.get(3), equalTo("test4"));
        assertThat(valuesTest1.get(4), equalTo("test5"));
        
        // ACT
        List<String> valuesTest2 = (List<String>) evalScriptGet(":project:server:test:test1", "0", "9");

        // ASSERT
        assertThat(valuesTest2.size(), equalTo(5));
        assertThat(valuesTest2.get(0), equalTo(TYPE_COLLECTION));
        assertThat(valuesTest2.get(1), equalTo("test2"));
        assertThat(valuesTest2.get(2), equalTo("test3"));
        assertThat(valuesTest2.get(3), equalTo("test4"));
        assertThat(valuesTest2.get(4), equalTo("test5"));
        
        // ACT
        List<String> valuesTest3 = (List<String>) evalScriptGet(":project:server:test:test1", "9", "4");

        // ASSERT
        assertThat(valuesTest3.size(), equalTo(1));
        assertThat(valuesTest3.get(0), equalTo(TYPE_COLLECTION));
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
                add("9999999999999");
                add(resourceValue1);
                add(UUID.randomUUID().toString());
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
                add("9999999999999");
                add(resourceValue1);
                add(UUID.randomUUID().toString());
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
                add("bla");
                add("bla");
            }
        }
                );
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptGet(final String resourceName1, final String offset, final String count) {
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
                add(offset);
                add(count);
                
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
}
