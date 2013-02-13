package li.chee.vertx.reststorage;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

public class RestStorageMod extends BusModBase {

    private Logger log;

    @Override
    public void start() {
        log = container.getLogger();
        final EventBus eb = vertx.eventBus();

        JsonObject config = container.getConfig();

        String storageName = config.getString("storage", "filesystem");
        int port = config.getNumber("port", 8989).intValue();
        String prefix = config.getString("prefix", "");
        prefix = prefix.equals("/") ? "" : prefix;

        Storage storage;
        switch (storageName) {
        case "filesystem":
            String root = config.getString("root", ".");
            storage = new FileSystemStorage(root);
            break;
        case "redis":
            if(config.getObject("redisConfig") != null) {
                container.deployModule("de.marx-labs.redis-client-v0.4", config.getObject("redisConfig"));
            }
            String redisAddress = config.getString("address", "redis-client");
            String redisPrefix = config.getString("root", "rest-storage");
            storage = new RedisStorage(redisAddress, redisPrefix);
            break;
        default:
            throw new RuntimeException("Storage not supported: "+storageName);
        }

        vertx.createHttpServer().requestHandler(new RestStorageHandler(storage, prefix)).listen(port);
    }
}
