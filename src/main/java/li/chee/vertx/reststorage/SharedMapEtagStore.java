package li.chee.vertx.reststorage;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.vertx.java.core.Vertx;

public class SharedMapEtagStore implements EtagStore {

    private Vertx vertx;
    
    public SharedMapEtagStore(Vertx vertx) {
       this.vertx = vertx;
    }

    @Override
    public String get(String path) {
        ConcurrentMap<String, String> map = getMap();        
        String etag = map.get(path);
        if(etag == null) {
            etag = UUID.randomUUID().toString();
            map.put(path, etag);
        }
        return etag;
    }

    @Override
    public void reset(String path) {
        getMap().remove(path);
    }
    
    private ConcurrentMap<String, String> getMap() {
        return vertx.sharedData().getMap("li.chee.rest-storage.etags");
    }
    
}
