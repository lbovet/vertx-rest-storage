package org.swisspush.reststorage.util;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link ResourceNameUtil} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class ResourceNameUtilTest {

    @Test
    public void testReplaceColonsAndSemiColons(TestContext testContext) {
        testContext.assertEquals(null, ResourceNameUtil.replaceColonsAndSemiColons(null));
        testContext.assertEquals("", ResourceNameUtil.replaceColonsAndSemiColons(""));
        testContext.assertEquals("StringWithoutColonsAndSemiColons", ResourceNameUtil.replaceColonsAndSemiColons("StringWithoutColonsAndSemiColons"));
        testContext.assertEquals("1_hello-@$&()*+,=-._~!'", ResourceNameUtil.replaceColonsAndSemiColons("1_hello-@$&()*+,=-._~!'"));
        testContext.assertEquals("1_hello_§_°_123", ResourceNameUtil.replaceColonsAndSemiColons("1_hello_:_;_123"));
        testContext.assertEquals("§§§°°°", ResourceNameUtil.replaceColonsAndSemiColons(":::;;;"));
    }

    @Test
    public void testReplaceColonsAndSemiColonsInList(TestContext testContext) {

        List<String> resources = Arrays.asList("res_1", "res_:_;_2", "", ":::;;;");

        ResourceNameUtil.replaceColonsAndSemiColonsInList(resources);

        testContext.assertEquals("res_1", resources.get(0));
        testContext.assertEquals("res_§_°_2", resources.get(1));
        testContext.assertEquals("", resources.get(2));
        testContext.assertEquals("§§§°°°", resources.get(3));
    }

    @Test
    public void testResetReplacedColonsAndSemiColons(TestContext testContext) {
        testContext.assertEquals(null, ResourceNameUtil.resetReplacedColonsAndSemiColons(null));
        testContext.assertEquals("", ResourceNameUtil.resetReplacedColonsAndSemiColons(""));
        testContext.assertEquals("StringWithoutColonsAndSemiColons", ResourceNameUtil.resetReplacedColonsAndSemiColons("StringWithoutColonsAndSemiColons"));
        testContext.assertEquals("1_hello-@$&()*+,=-._~!'", ResourceNameUtil.resetReplacedColonsAndSemiColons("1_hello-@$&()*+,=-._~!'"));
        testContext.assertEquals("1_hello_:_;_123", ResourceNameUtil.resetReplacedColonsAndSemiColons("1_hello_§_°_123"));
        testContext.assertEquals(":::;;;", ResourceNameUtil.resetReplacedColonsAndSemiColons("§§§°°°"));
    }

    @Test
    public void testResetReplacedColonsAndSemiColonsInList(TestContext testContext) {

        List<String> resources = Arrays.asList("res_1", "res_§_°_2", "", "§§§°°°");

        ResourceNameUtil.resetReplacedColonsAndSemiColonsInList(resources);

        testContext.assertEquals("res_1", resources.get(0));
        testContext.assertEquals("res_:_;_2", resources.get(1));
        testContext.assertEquals("", resources.get(2));
        testContext.assertEquals(":::;;;", resources.get(3));
    }
}
