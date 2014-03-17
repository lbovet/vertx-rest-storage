package li.chee.vertx.reststorage;

import org.vertx.java.core.Handler;

public interface Storage {

    void get(String path, Handler<Resource> handler);

    void put(String path, boolean merge, long expire, Handler<Resource> handler);

    void delete(String path, Handler<Resource> handler);

    void cleanup(Handler<DocumentResource> handler);

}
