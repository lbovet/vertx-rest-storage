package li.chee.vertx.reststorage;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    RouteMatcher routeMatcher = new RouteMatcher();
    
    public RestStorageHandler(final Storage storage, final String prefix) {        
        routeMatcher.getWithRegEx(prefix+".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = stripTrailingSlash(request.path.substring(prefix.length()));
                storage.get(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result; 
                        if(resource.exists) {
                            if(resource instanceof CollectionResource) {  
                                CollectionResource collection = (CollectionResource)resource;                                
                                JsonArray array = new JsonArray();
                                for(String s : collection.items) {
                                    array.addString(s);
                                }                      
                                String body = new JsonObject().putArray(collectionName(path), array).encode();
                                request.response.headers().put("Content-Length", body.length());
                                request.response.headers().put("Content-Type", "application/json; charset=utf-8");
                                request.response.write(body).end();                                
                            }                            
                        }
                    }                    
                });
            }
        });
    }
    
    @Override
    public void handle(HttpServerRequest request) {
        routeMatcher.handle(request);
    }  

    private String stripTrailingSlash(String value) {
        while(value.endsWith("/")) {
            value = value.substring(0, value.length()-1);
        }
        return value;
    }
    
    private String collectionName(String path) {
        if(path.equals("/") && path.equals("")) {
            return "items";
        } else {
            return path.substring(path.lastIndexOf("/")+1);
        }
    }
}
