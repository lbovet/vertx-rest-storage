package li.chee.vertx.reststorage;

import java.util.Arrays;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;
import org.vertx.java.deploy.impl.VertxLocator;

public class StaticStorage implements Storage {

    public void get(String path, Handler<AsyncResult<Resource>> handler) {
        System.out.println("GET "+path);
        if (path.equals("/dogs")) {
            CollectionResource c = new CollectionResource();
            c.items = Arrays.asList(new String[] { "hello", "world" });
            handler.handle(new AsyncResult<Resource>(c));
        } else if (path.equals("/hello.txt")) {
            DocumentResource d = new DocumentResource();
            final String document = "Hello, World";
            d.length = document.length();
            d.closeHandler = new SimpleHandler() {
                protected void handle() {
                    // nothing to close                    
                }
            };
            d.readStream = new ReadStream() {
                boolean paused;
                int position;
                Handler<Void> endHandler;
                Handler<Buffer> dataHandler;
                                
                private void doRead() {
                    VertxLocator.vertx.runOnLoop(new SimpleHandler() {
                        protected void handle() {
                            if(!paused) {
                                if(position < document.length()) {
                                    dataHandler.handle(new Buffer(""+document.charAt(position)));
                                    position++;
                                    doRead();
                                } else {
                                    endHandler.handle(null);
                                }
                            }
                        }
                    });
                }
                
                public void resume() {
                    paused = false;
                    doRead();
                }
                
                @Override
                public void pause() {
                    paused = true;
                }
                
                @Override
                public void exceptionHandler(Handler<Exception> handler) {
                }
                
                @Override
                public void endHandler(Handler<Void> endHandler) {
                    this.endHandler = endHandler;
                    
                }
                
                @Override
                public void dataHandler(Handler<Buffer> handler) {
                    this.dataHandler = handler;                   
                    doRead();
                }
            }; 
            handler.handle(new AsyncResult<Resource>(d));
        } else {
            Resource r = new Resource();
            r.exists = false;
            handler.handle(new AsyncResult<Resource>(r));
        }
            
        
    }

    @Override
    public void put(String path, Handler<AsyncResult<Resource>> handler) {
        System.out.println("PUT "+path);
        if (path.equals("/dogs")) {
            CollectionResource c = new CollectionResource();
            handler.handle(new AsyncResult<Resource>(c));
        } else if (path.equals("/hello")) {
            final StringBuilder b = new StringBuilder();
            DocumentResource d = new DocumentResource();
            d.closeHandler = new SimpleHandler() {
                protected void handle() {
                    System.out.println(b.toString());
                }                
            };
            d.writeStream = new WriteStream() {
                
                @Override
                public boolean writeQueueFull() {
                    return false;
                }
                
                @Override
                public void writeBuffer(Buffer data) {
                    System.out.println("got "+data.toString());
                    b.append(data.toString());           
                }
                
                @Override
                public void setWriteQueueMaxSize(int maxSize) {                    
                }
                
                @Override
                public void exceptionHandler(Handler<Exception> handler) {                   
                }
                
                @Override
                public void drainHandler(Handler<Void> handler) {
                    // TODO Auto-generated method stub                    
                }
            };
            handler.handle(new AsyncResult<Resource>(d));
        }
    }

    @Override
    public void delete(String path, Handler<AsyncResult<Resource>> handler) {
        // TODO Auto-generated method stub
        
    }

}
