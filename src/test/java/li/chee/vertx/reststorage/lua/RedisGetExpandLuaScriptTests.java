/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package li.chee.vertx.reststorage.lua;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RedisGetExpandLuaScriptTests extends AbstractLuaScriptTest {

    @Test
    public void testGetExpand() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item2", "item1", "item3");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(3));
        assertThat(value.get(0), equalTo("{\"content\": \"content_2\"}"));
        assertThat(value.get(1), equalTo("{\"content\": \"content_1\"}"));
        assertThat(value.get(2), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testGetExpandEmptySubresources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", new ArrayList<String>());

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    @Test
    public void testGetExpandWithSubCollections() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}");

        // ACT
        List<String> subResources = Arrays.asList("sub/", "item1", "item2", "item3");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(4));
        assertThat(value.get(0), equalTo("[\"sub1\",\"sub2\"]"));
        assertThat(value.get(1), equalTo("{\"content\": \"content_1\"}"));
        assertThat(value.get(2), equalTo("{\"content\": \"content_2\"}"));
        assertThat(value.get(3), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testGetExpandWithSubSubCollections() {

        // ARRANGE
        evalScriptPut(":project:server:test:sub:subsub:item1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:subsub:item2", "{\"content\": \"content_sub_2\"}");

        // ACT
        List<String> subResources = Arrays.asList("sub/");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0), equalTo("[\"subsub\"]"));

        // ACT
        subResources = Arrays.asList("subsub/");
        value = (List<String>) evalScriptGetExpand(":project:server:test:sub", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0), equalTo("[\"item1\",\"item2\"]"));

        // ACT
        subResources = Arrays.asList("item1", "item2");
        value = (List<String>) evalScriptGetExpand(":project:server:test:sub:subsub", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0), equalTo("{\"content\": \"content_sub_1\"}"));
        assertThat(value.get(1), equalTo("{\"content\": \"content_sub_2\"}"));
    }

    @Test
    public void testGetExpandWithCollectionsOnly() {

        // ARRANGE
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}");
        evalScriptPut(":project:server:test:anothersub:sub3", "{\"content\": \"content_sub_3\"}");
        evalScriptPut(":project:server:test:anothersub:sub4", "{\"content\": \"content_sub_4\"}");

        // ACT
        List<String> subResources = Arrays.asList("sub/", "anothersub/");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0), equalTo("[\"sub1\",\"sub2\"]"));
        assertThat(value.get(1), equalTo("[\"sub3\",\"sub4\"]"));
    }

    @Test
    public void testGetExpandWithNoMatchingResources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item2x", "item1x", "item3x");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    @Test
    public void testGetExpandWithInvalidSubresources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item3", "invalidItem", "item1");
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0), equalTo("{\"content\": \"content_3\"}"));
        assertThat(value.get(1), equalTo("{\"content\": \"content_1\"}"));

        // ACT
        subResources = Arrays.asList("item3", "invalidSub/");
        value = (List<String>) evalScriptGetExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testGetExpandExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", now);

        String nowPlus100 = String.valueOf(System.currentTimeMillis() + 100);
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}", nowPlus100);

        Thread.sleep(15);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", Arrays.asList("item1", "item2"), timestamp);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0), equalTo("{\"content\": \"content_2\"}"));

        Thread.sleep(100);

        // ACT
        timestamp = String.valueOf(System.currentTimeMillis());
        value = (List<String>) evalScriptGetExpand(":project:server:test", Arrays.asList("item1", "item2"), timestamp);

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    @Test
    public void testGetExpandExpiredWithCollections() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000 = String.valueOf(System.currentTimeMillis() + 1000);

        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", now);
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}", now);
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}", nowPlus1000);
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}", now);
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}", nowPlus1000);

        Thread.sleep(15);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        List<String> value = (List<String>) evalScriptGetExpand(":project:server:test", Arrays.asList("sub/", "item1", "item2", "item3"), timestamp);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0), equalTo("[\"sub2\"]"));
        assertThat(value.get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    @SuppressWarnings({"rawtypes", "unchecked", "serial"})
    private Object evalScriptGetExpand(final String resourceName1, final List<String> subResources) {
        return evalScriptGetExpand(resourceName1, subResources, String.valueOf(System.currentTimeMillis()));
    }

    @SuppressWarnings({"rawtypes", "unchecked", "serial"})
    private Object evalScriptGetExpand(final String resourceName1, final List<String> subResources, final String timestamp) {
        String getScript = readScript("getExpand.lua");
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
                        add(StringUtils.join(subResources, ";"));
                        add(String.valueOf(subResources.size()));
                    }
                }
        );
    }
}
