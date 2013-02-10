package li.chee.vertx.reststorage;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;
import org.vertx.java.framework.TestUtils;

public class TestServer extends Verticle {    
    
    private TestUtils tu;
    

    @Override
    public void start() throws Exception {
        final Logger log = container.getLogger();

        final EventBus eb = vertx.eventBus();
        
        tu = new TestUtils(vertx);
        
        container.deployModule("li.chee.rest-storage-v0.1", 
                new JsonObject() {{ putString("prefix", "/test"); putString("root", "dogs");}}, 1, new Handler<String>() {
            public void handle(String event) {
                tu.appReady();
            }
        });        
    }

    @Override
    public void stop() throws Exception {
        tu.appStopped();
    }

    
}
