package li.chee.vertx.reststorage;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.testframework.TestClientBase;


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
        Handler<Void> next;
        
        public PrintHandler() {            
        }
        
        public PrintHandler(boolean complete) {
            this.complete = complete;
        }
        
        public PrintHandler(Handler<Void> next) {
            this.next = next;
        }

        public void handle(HttpClientResponse response) {
            System.out.println(response.statusCode);
            Map<String, String> headers = new HashMap<>();
            headers.putAll(response.headers());
            System.out.println(headers);
            response.bodyHandler(new Handler<Buffer>() {
                public void handle(Buffer body) {
                    System.out.println(body.toString());
                    if(next != null) {
                        next.handle(null);
                    }
                    if (complete) {
                        tu.testComplete();
                    }
                }
            });
        }
    }

    HttpClient client;
    
    public void testSimple() {
        client = vertx.createHttpClient().setPort(8989);
        HttpClientRequest request = client.get("/test/", new PrintHandler(step2()));
        request.headers().put("Accept", "text/html");
        request.end();
    }
    
    private Handler<Void> step2() {
        return new Handler<Void>() {
            public void handle(Void event) {
                StringBuilder content = new StringBuilder("{");
                for(int i=0; i<10000; i++) {
                     content.append(" \"hello\": \"world\",\n");
                }
                content.append("\"hello\": \"world\" }");
                HttpClientRequest request = client.put("/test/hello", new PrintHandler(step3()));
                request.headers().put("Content-Length", content.length());
                request.write(content.toString());
                request.end();
            }            
        };
    }
    
    private Handler<Void> step3() {
        return new Handler<Void>() {
            public void handle(Void event) {
                String content = "{ \"hello\": \"world\" }";
                HttpClientRequest request = client.put("/test/world", new PrintHandler(step4()));
                request.headers().put("Content-Length", content.length());
                request.write(content);
                request.end();
            }            
        };
    }
    
    private Handler<Void> step4() {
        return new Handler<Void>() {
            public void handle(Void event) {
                HttpClientRequest request = client.delete("/test/world", new PrintHandler(true));
                request.end();
            }            
        };
    }
}
