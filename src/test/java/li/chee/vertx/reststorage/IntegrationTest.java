package li.chee.vertx.reststorage;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class IntegrationTest {

    Vertx vertx;
    HttpClient client;
    private Logger log = LoggerFactory.getLogger(IntegrationTest.class);

    @Before
    public void setUp(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject storageConfig = new JsonObject();
        storageConfig.put("storage", "redis");
        storageConfig.put("storageAddress", "we-love-to-put");
        storageConfig.put("prefix", "/test");
        vertx.deployVerticle("li.chee.vertx.reststorage.RestStorageMod", new DeploymentOptions().setConfig(storageConfig), context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testSimple(TestContext context) {
        Async async = context.async();
        client = vertx.createHttpClient(new HttpClientOptions().setDefaultPort(8989));
        HttpClientRequest request = client.get("/test/", new PrintHandler(step2(async), async));
        request.headers().add("Accept", "text/html");
        request.end();
    }

    class PrintHandler implements Handler<HttpClientResponse> {
        boolean complete;
        Handler<Void> next;
        Async async;
        
        public PrintHandler() {            
        }
        
        public PrintHandler(boolean complete, Async async) {
            this.complete = complete;
            this.async = async;
        }
        
        public PrintHandler(Handler<Void> next, Async async) {
            this.next = next;
            this.async = async;
        }

        public void handle(HttpClientResponse response) {
            log.info(response.statusCode());
            response.bodyHandler(body -> {
                log.info(body.toString());
                if(next != null) {
                    next.handle(null);
                }
                if (complete) {
                    async.complete();
                }
            });
        }
    }

    private Handler<Void> step2(Async async) {
        return event -> {
            StringBuilder content = new StringBuilder("{");
            for(int i=0; i<10000; i++) {
                 content.append(" \"hello\": \"world\",\n");
            }
            content.append("\"hello\": \"world\" }");
            HttpClientRequest request = client.put("/test/hello", new PrintHandler(step3(async), async));
            request.headers().add("Content-Length", ""+content.length());
            request.write(content.toString());
            request.end();
        };
    }
    
    private Handler<Void> step3(Async async) {
        return event -> {
            String content = "{ \"hello\": \"world\" }";
            HttpClientRequest request = client.put("/test/world", new PrintHandler(step4(async), async));
            request.headers().add("Content-Length", ""+content.length());
            request.write(content);
            request.end();
        };
    }
    
    private Handler<Void> step4(Async async) {
        return event -> {
            HttpClientRequest request = client.delete("/test/world", new PrintHandler(true, async));
            request.end();
        };
    }
}
