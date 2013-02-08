package li.chee.vertx.reststorage;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.logging.Logger;

public class RestStorage extends BusModBase {

    private Logger log;
 
    @Override
    public void start() {
        log = container.getLogger();
        final EventBus eb = vertx.eventBus();

        vertx.createHttpServer().requestHandler(new RestStorageHandler(new StaticStorage(), "/test")).listen(8989);
    }
}
