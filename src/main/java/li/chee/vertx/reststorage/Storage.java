package li.chee.vertx.reststorage;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

public interface Storage {

    void get(String path, Handler<AsyncResult<Resource>> handler);

}
