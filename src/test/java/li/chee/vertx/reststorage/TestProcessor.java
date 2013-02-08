package li.chee.vertx.reststorage;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;
import org.vertx.java.framework.TestUtils;

public class TestProcessor extends Verticle {    
    
    private TestUtils tu;
    

    @Override
    public void start() throws Exception {
        final Logger log = container.getLogger();

        final EventBus eb = vertx.eventBus();
        
        tu = new TestUtils(vertx);
        
        tu.appReady();
    }

    @Override
    public void stop() throws Exception {
        tu.appStopped();
    }

    
}
