package li.chee.vertx.reststorage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class RestStorageMod extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    @Override
    public void start(Future<Void> fut) {

        JsonObject config = config();

        String storageName = config.getString("storage", "filesystem");
        int port = config.getInteger("port", 8989);
        String prefix = config.getString("prefix", "");
        String storageAddress = config.getString("storageAddress", "resource-storage");
        JsonObject editorConfig = config.getJsonObject("editors");
        prefix = prefix.equals("/") ? "" : prefix;

        Storage storage;
        switch (storageName) {
        case "filesystem":
            String root = config.getString("root", ".");
            storage = new FileSystemStorage(vertx, root);
            break;
        case "redis":
            storage = new RedisStorage(vertx, log, config);
            break;
        default:
            throw new RuntimeException("Storage not supported: " + storageName);
        }

        Handler<HttpServerRequest> handler = new RestStorageHandler(vertx, log, storage, prefix, editorConfig);
        vertx.createHttpServer().requestHandler(handler).listen(port, result -> {
            if(result.succeeded()){
                new EventBusAdapter().init(vertx, storageAddress, handler);
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });
    }
}
