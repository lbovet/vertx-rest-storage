package li.chee.vertx.reststorage;

import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class RestStorageMod extends Verticle {

    @Override
    public void start() {

        JsonObject config = container.config();

        String storageName = config.getString("storage", "filesystem");
        int port = config.getNumber("port", 8989).intValue();
        String prefix = config.getString("prefix", "");
        JsonObject editorConfig = config.getObject("editors");
        prefix = prefix.equals("/") ? "" : prefix;

        Storage storage;
        String etag;
        switch (storageName) {
        case "filesystem":
            String root = config.getString("root", ".");
            storage = new FileSystemStorage(vertx, root);
            etag = config.getString("etag", "none");
            break;
        case "redis":
            if (config.getObject("redisConfig") != null) {
                container.deployModule("io.vertx~mod-redis~1.1.3", config.getObject("redisConfig"));
            }
            String redisAddress = config.getString("address", "redis-client");
            String redisResourcesPrefix = config.getString("root", "rest-storage:resources");
            String redisCollectionsPrefix = config.getString("root", "rest-storage:collections");
            storage = new RedisStorage(vertx, redisAddress, redisResourcesPrefix, redisCollectionsPrefix);
            etag = config.getString("etag", "memory");
            break;
        default:
            throw new RuntimeException("Storage not supported: " + storageName);
        }

        EtagStore etagStore = etag.equals("memory") ? new SharedMapEtagStore(vertx) : new EmptyEtagStore();
        vertx.createHttpServer().requestHandler(new RestStorageHandler(storage, etagStore, prefix, editorConfig)).listen(port);
    }
}
