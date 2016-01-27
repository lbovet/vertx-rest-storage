package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import com.jayway.restassured.parsing.Parser;
import com.jayway.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import io.vertx.core.json.JsonObject;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractTestCase {

    Vertx vertx;
    private final String address = "test.redis.client";

    // restAssured Configuration
    private static final int REST_STORAGE_PORT = 8989;
    private static RequestSpecification REQUEST_SPECIFICATION = new RequestSpecBuilder()
            .addHeader("content-type", "application/json")
            .setPort(8989)
            .setBasePath("/")
            .build();

    @BeforeClass
    public static void config() {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.start();
        }
    }

    @AfterClass
    public static void stopRedis() {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.stop();
        }
    }

    @Before
    public void setUp(TestContext context) {
        vertx = Vertx.vertx();

        // RestAssured Configuration
        RestAssured.port = REST_STORAGE_PORT;
        RestAssured.requestSpecification = REQUEST_SPECIFICATION;
        RestAssured.registerParser("application/json; charset=utf-8", Parser.JSON);
        RestAssured.defaultParser = Parser.JSON;

        JsonObject storageConfig = new JsonObject();
        storageConfig.put("storage", "redis");
        storageConfig.put("storageAddress", "rest-storage");
        storageConfig.put("redisAddress", address);
        storageConfig.put("redisHost", "localhost");
        storageConfig.put("redisPort", 6379);
        vertx.deployVerticle("li.chee.vertx.reststorage.RestStorageMod", new DeploymentOptions().setConfig(storageConfig), context.asyncAssertSuccess(stringAsyncResult1 -> {
            // standard code: will called @Before every test
            RestAssured.basePath = "";
        }));
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}