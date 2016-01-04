package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import com.jayway.restassured.parsing.Parser;
import com.jayway.restassured.specification.RequestSpecification;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

public abstract class AbstractTestCase extends TestVerticle {
    private final String address = "test.redis.client";
    private EventBus eb;
    Logger log = null;
    // restAssured Configuration
    private static final int REST_STORAGE_PORT = 8989;
    private static RequestSpecification REQUEST_SPECIFICATION = new RequestSpecBuilder()
            .addHeader("content-type", "application/json")
            .setPort(8989)
            .setBasePath("/")
            .build();

    private void appReady() {
        super.start();
    }

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

    public void start() {
        // RestAssured Configuration
        RestAssured.port = REST_STORAGE_PORT;
        RestAssured.requestSpecification = REQUEST_SPECIFICATION;
        RestAssured.registerParser("application/json; charset=utf-8", Parser.JSON);
        RestAssured.defaultParser = Parser.JSON;

        // initialize vertx and eventBus
        log = container.logger();
        VertxAssert.initialize(vertx);
        eb = vertx.eventBus();

        // config for vert.x-mod-redis module
        JsonObject config = new JsonObject();
        config.putString("address", address);
        config.putString("host", "localhost");
        config.putNumber("port", 6379);
        config.putString("encoding", "ISO-8859-1");

        // deploy vert.x-mod-redis module
        container.deployModule("io.vertx~mod-redis~1.1.3", config, 1, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> event) {
                JsonObject storageConfig = new JsonObject();
                storageConfig.putString("storage", "redis");
                storageConfig.putString("storageAddress", "rest-storage");
                storageConfig.putString("redisAddress", address);
                storageConfig.putString("redisConfig", null);
                // deploy reststorage module
                container.deployModule(System.getProperty("vertx.modulename"), storageConfig, 1, new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.failed()) {
                            log.error("Could not load main redis module", event.cause());
                            return;
                        }

                        // standard code: will called @Before every test
                        RestAssured.basePath = "";

                        appReady();
                    }
                });
            }
        });
    }
}