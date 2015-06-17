package li.chee.vertx.reststorage;

import org.vertx.java.core.Handler;

public interface Storage {

    void get(String path, String etag, int offset, int count, Handler<Resource> handler);

    void put(String path, String etag, boolean merge, long expire, Handler<Resource> handler);

    void delete(String path, Handler<Resource> handler);

    void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmount);

}
