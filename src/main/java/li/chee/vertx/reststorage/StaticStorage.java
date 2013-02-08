package li.chee.vertx.reststorage;

import java.util.Arrays;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

public class StaticStorage implements Storage {

    public void get(String path, Handler<AsyncResult<Resource>> handler) {    
        CollectionResource c = new CollectionResource();
        c.items = Arrays.asList(new String[] { "hello", "world" });
        handler.handle(new AsyncResult<Resource>(c));        
    }

}
