package li.chee.vertx.reststorage;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.framework.TestClientBase;

public class TestClient extends TestClientBase {

    private EventBus eb;

    @Override
    public void start() {
        super.start();
        eb = vertx.eventBus();
        tu.appReady();
    }

    @Override
    public void stop() {
        super.stop();
    }

    public void testSimple() {
        
        vertx.createHttpClient().setPort(8989).getNow("/test/dogs", new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse response) {
                System.out.println(response.statusCode);
                Map<String,String> headers = new HashMap<>();
                headers.putAll(response.headers());
                System.out.println(headers);
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        System.out.println(body.toString());
                        tu.testComplete();
                    }
                });
            }
        });       
    }
    
}
