package li.chee.vertx.reststorage;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
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

    class PrintHandler implements Handler<HttpClientResponse> {
        boolean complete;

        public PrintHandler(boolean complete) {
            this.complete = complete;
        }

        public void handle(HttpClientResponse response) {
            System.out.println(response.statusCode);
            Map<String, String> headers = new HashMap<>();
            headers.putAll(response.headers());
            System.out.println(headers);
            response.bodyHandler(new Handler<Buffer>() {
                public void handle(Buffer body) {
                    System.out.println(body.toString());
                    if (complete) {
                        tu.testComplete();
                    }
                }
            });
        }
    }

    public void testSimple() {
        testFileSystemStorage();
        // testStaticStorage();
    }
    
    private void testFileSystemStorage() {
        HttpClient client = vertx.createHttpClient().setPort(8989);
        // client.getNow("/test/build", new PrintHandler(true));
        
//        String content = "{ \"hello\": \"world\" }";
//        HttpClientRequest request = client.put("/test/dogs/hello", new PrintHandler(true));
//        request.headers().put("Content-Length", content.length());
//        request.write(content);
//        request.end();
        
        HttpClientRequest request = client.delete("/test/dogs/hello", new PrintHandler(true));
        request.end();
    }

    private void testStaticStorage() {
        HttpClient client = vertx.createHttpClient().setPort(8989);
        
        //client.getNow("/test/dogs", new PrintHandler(false));
        
        //client.getNow("/test/hello.txt", new PrintHandler(false));
        
        String content = "{ \"hello\": \"world\" }";
        HttpClientRequest request = client.put("/test/dogs", new PrintHandler(false));
        request.headers().put("Content-Length", content.length());
        request.write(content);
        request.end();
        
        HttpClientRequest request2 = client.put("/test/hello", new PrintHandler(true));
        request2.headers().put("Content-Length", content.length());
        request2.write(content);
        request2.end();
    }
}
