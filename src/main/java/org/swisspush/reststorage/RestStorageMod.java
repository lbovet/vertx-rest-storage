package org.swisspush.reststorage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

public class RestStorageMod extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    @Override
    public void start(Future<Void> fut) {
        ModuleConfiguration modConfig = ModuleConfiguration.fromJsonObject(config());
        log.info("Starting RestStorageMod with configuration: " + modConfig);
        Storage storage;
        switch (modConfig.getStorageType()) {
            case filesystem:
                storage = new FileSystemStorage(vertx, modConfig.getRoot());
                break;
            case redis:
                storage = new RedisStorage(vertx, modConfig);
                break;
            default:
                throw new RuntimeException("Storage not supported: " + modConfig.getStorageType());
        }

        Handler<HttpServerRequest> handler = new RestStorageHandler(vertx, log, storage, modConfig.getPrefix(), modConfig.getEditorConfig());
        vertx.createHttpServer().requestHandler(handler).listen(modConfig.getPort(), result -> {
            if(result.succeeded()){
                new EventBusAdapter().init(vertx, modConfig.getStorageAddress(), handler);
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });
    }
}
