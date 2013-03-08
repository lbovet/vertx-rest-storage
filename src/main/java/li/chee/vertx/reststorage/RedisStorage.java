package li.chee.vertx.reststorage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;
import org.vertx.java.deploy.impl.VertxLocator;

public class RedisStorage implements Storage {

    private String redisAddress;
    private String redisPrefix;
    private EventBus eb;
    private String mergeScript;
    
    public RedisStorage(String redisAddress, String redisPrefix) {
        this.redisAddress = redisAddress;
        this.redisPrefix = redisPrefix;
        eb = VertxLocator.vertx.eventBus();
        
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("json-merge.lua")));
        StringBuilder sb;
        try {            
            sb = new StringBuilder();
            String line;
            while( (line = in.readLine()) != null ) {
                sb.append(line+"\n");
            }
            
        } catch (IOException e) {            
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        mergeScript = sb.toString();
    }

    public class ByteArrayReadStream implements ReadStream {

        ByteArrayInputStream content;
        int size;
        boolean paused;
        int position;
        Handler<Void> endHandler;
        Handler<Buffer> dataHandler;

        public ByteArrayReadStream(byte[] byteArray) {
            size = byteArray.length;
            content = new ByteArrayInputStream(byteArray);
        }

        private void doRead() {
            VertxLocator.vertx.runOnLoop(new SimpleHandler() {
                protected void handle() {
                    if (!paused) {
                        if (position < size) {
                            int toRead = 8192;
                            if (position + toRead > size) {
                                toRead = size - position;
                            }
                            byte[] bytes = new byte[toRead];
                            content.read(bytes, 0, toRead);
                            dataHandler.handle(new Buffer(bytes));
                            position += toRead;
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
    }

    @Override
    public void get(String path, final Handler<AsyncResult<Resource>> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putString("pattern", key + "*");
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body.getArray("value");
                if (list.size() == 0) {
                    notFound(handler);
                    return;
                }
                boolean collection = false;
                for (Object o : list) {
                    String subKey = ((String) o);
                    if(subKey.startsWith(key+":")) {
                        collection=true;
                    }
                }
                if (!collection) { // Document
                    if(!list.contains(key)) {
                        notFound(handler);
                        return;
                    }
                    JsonObject command = new JsonObject();
                    command.putString("command", "get");
                    command.putString("key", key);
                    eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> event) {                            
                            String value = event.body.getString("value");
                            if (value != null) {
                                DocumentResource r = new DocumentResource();
                                byte[] content = decodeBinary(value);
                                r.readStream = new ByteArrayReadStream(content);
                                r.length = content.length;
                                r.closeHandler = new Handler<Void>() {
                                    public void handle(Void event) {
                                        // nothing to close
                                    }                                    
                                };
                                handler.handle(new AsyncResult<Resource>(r));
                            } else {
                                notFound(handler);
                            }
                        }
                    });
                    return;
                } else { // Collection
                    CollectionResource r = new CollectionResource();
                    Set<Resource> items = new HashSet<>();
                    for (Object o : list) {
                        String subKey = (String)o;
                        // skip bogous parent collections that are also documents and non-children
                        if(subKey.equals(key) || subKey.charAt(key.length()) != ':') {
                            continue;
                        }
                        String subPath = decodePath(subKey).substring(decodePath(key).length()+1);
                        if (subPath.contains("/")) {
                            CollectionResource c = new CollectionResource();
                            c.name = subPath.split("/")[0];
                            items.add(c);
                        } else {
                            DocumentResource d = new DocumentResource();
                            d.name = subPath;
                            items.add(d);
                        }
                    }                    
                    if(collection) {                                  
                        r.items = new ArrayList<Resource>(items);
                        Collections.sort(r.items);
                        handler.handle(new AsyncResult<Resource>(r));
                    } else {
                        notFound(handler);
                    }
                }
            }
        });
    }

    class ByteArrayWriteStream implements WriteStream {

        private ByteArrayOutputStream bos = new ByteArrayOutputStream();

        public byte[] getBytes() {
            return bos.toByteArray();
        }

        @Override
        public void writeBuffer(Buffer data) {
            try {
                bos.write(data.getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setWriteQueueMaxSize(int maxSize) {
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public void drainHandler(Handler<Void> handler) {
        }

        @Override
        public void exceptionHandler(Handler<Exception> handler) {
        }
    }

    @Override
    public void put(String path, final boolean merge, final Handler<AsyncResult<Resource>> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putString("pattern", key + "*");
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body.getArray("value");
                boolean collection = false;
                for (Object o : list) {
                    String subKey = ((String) o);
                    if (subKey.startsWith(key + ":")) {
                        collection = true;
                    }
                }
                if (!collection) {
                    final DocumentResource d = new DocumentResource();
                    final ByteArrayWriteStream stream = new ByteArrayWriteStream();
                    d.writeStream = stream;
                    d.closeHandler = new Handler<Void>() {
                        public void handle(Void event) {
                            JsonObject command = new JsonObject();
                            if(merge) {
                                command.putString("command", "eval");
                                command.putString("script", mergeScript);
                                command.putNumber("numkeys", 1);
                                command.putArray("key", new JsonArray( new Object[] { key }));
                                command.putArray("arg", new JsonArray( new Object[] { encodeBinary(stream.getBytes())} ));
                            } else {
                                command.putString("command", "set");
                                command.putString("key", key);
                                command.putString("value", encodeBinary(stream.getBytes()));
                            }
                            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                public void handle(Message<JsonObject> event) {
                                    if("error".equals(event.body.getString("status")) && d.errorHandler != null) {
                                        d.errorHandler.handle(event.body.getString("message"));
                                    } else {
                                        d.endHandler.handle(null);
                                    }
                                }
                            });
                        }
                    };
                    handler.handle(new AsyncResult<Resource>(d));
                } else {
                    CollectionResource c = new CollectionResource();
                    handler.handle(new AsyncResult<Resource>(c));
                }
            }
        });
    }

    @Override
    public void delete(String path, final Handler<AsyncResult<Resource>> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putString("pattern", key + "*");
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body.getArray("value");
                if (list.size() == 0) {
                    notFound(handler);
                    return;
                }
                Iterator<Object> it = list.iterator();
                while(it.hasNext()) {
                    String item = (String)it.next(); 
                    if(!item.equals(key) && item.charAt(key.length()) != ':') {
                        it.remove();
                    }
                }
                JsonObject command = new JsonObject();
                command.putString("command", "del");
                command.putArray("keys", list);
                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        Resource r = new Resource();
                        handler.handle(new AsyncResult<Resource>(r));
                    }
                });
            }
        });
    }

    private String encodePath(String path) {
        if(path.equals("/")) {
            path="";
        }
        return redisPrefix + path.replaceAll(":", "§").replaceAll("/", ":");
    }

    private String decodePath(String key) {
        return key.replaceAll(":", "/").replaceAll("§", ":").substring(redisPrefix.length());
    }

    private String encodeBinary(byte[] bytes) {
        try {
            return new String(bytes, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] decodeBinary(String s) {
        try {
            return s.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private void notFound(Handler<AsyncResult<Resource>> handler) {
        Resource r = new Resource();
        r.exists = false;
        handler.handle(new AsyncResult<Resource>(r));
    }
}
