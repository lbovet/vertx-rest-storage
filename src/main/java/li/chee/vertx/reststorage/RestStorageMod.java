package li.chee.vertx.reststorage;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

public class RestStorageMod extends Verticle {

    @Override
    public void start() {

        JsonObject config = container.config();
        Logger log = container.logger();

        String storageName = config.getString("storage", "filesystem");
        int port = config.getNumber("port", 8989).intValue();
        String prefix = config.getString("prefix", "");
        String storageAddress = config.getString("storageAddress", "resource-storage");
        JsonObject editorConfig = config.getObject("editors");
        prefix = prefix.equals("/") ? "" : prefix;

        Storage storage;
        switch (storageName) {
        case "filesystem":
            String root = config.getString("root", ".");
            storage = new FileSystemStorage(vertx, root);
            break;
        case "redis":
            if (config.getObject("redisConfig") != null) {
                container.deployModule("io.vertx~mod-redis~1.1.3", config.getObject("redisConfig"));
            }
            String redisAddress = config.getString("redisAddress", "redis-client");
            String redisResourcesPrefix = config.getString("resourcesPrefix", "rest-storage:resources");
            String redisCollectionsPrefix = config.getString("collectionsPrefix", "rest-storage:collections");
            String redisDeltaResourcesPrefix = config.getString("deltaResourcesPrefix", "delta:resources");
            String redisDeltaEtagsPrefix = config.getString("deltaEtagsPrefix", "delta:etags");
            String expirableSet = config.getString("expirablePrefix", "rest-storage:expirable");
            Long cleanupResourcesAmount = config.getLong("resourceCleanupAmount", 100000);
            storage = new RedisStorage(vertx, log, redisAddress, redisResourcesPrefix, redisCollectionsPrefix, redisDeltaResourcesPrefix, redisDeltaEtagsPrefix, expirableSet, cleanupResourcesAmount);
            break;
        default:
            throw new RuntimeException("Storage not supported: " + storageName);
        }

        Handler<HttpServerRequest> handler = new RestStorageHandler(log, storage, prefix, editorConfig);
        vertx.createHttpServer().requestHandler(handler).listen(port);
        new EventBusAdapter().init(vertx, storageAddress, handler);
    }
}
