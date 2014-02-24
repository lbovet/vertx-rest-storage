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

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

public class RedisStorage implements Storage {

    private String redisAddress;
    private String redisPrefix;
    private EventBus eb;
    private Vertx vertx;
    private String mergeScript;
    
    public RedisStorage(Vertx vertx, String redisAddress, String redisPrefix) {
        this.redisAddress = redisAddress;
        this.redisPrefix = redisPrefix;
        eb = vertx.eventBus();
        this.vertx = vertx;
        
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

    public class ByteArrayReadStream implements ReadStream<ByteArrayReadStream> {

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
            vertx.runOnContext(new Handler<Void>() {
                public void handle(Void v) {
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

        public ByteArrayReadStream resume() {
            paused = false;
            doRead();
            return this;
        }

        @Override
        public ByteArrayReadStream pause() {
            paused = true;
            return this;
        }

        @Override
        public ByteArrayReadStream exceptionHandler(Handler<Throwable> handler) {
        	return this;
        }

        @Override
        public ByteArrayReadStream endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;

        }

        @Override
        public ByteArrayReadStream dataHandler(Handler<Buffer> handler) {
            this.dataHandler = handler;
            doRead();
            return this;
        }
    }

    @Override
    public void get(String path, final Handler<Resource> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putArray("args", new JsonArray().add(key + "*"));
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body().getArray("value");
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
                    command.putArray("args", new JsonArray().add(key));
                    eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> event) {                            
                            String value = event.body().getString("value");
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
                                handler.handle(r);
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
                        handler.handle(r);
                    } else {
                        notFound(handler);
                    }
                }
            }
        });
    }

    class ByteArrayWriteStream implements WriteStream<ByteArrayWriteStream> {

        private ByteArrayOutputStream bos = new ByteArrayOutputStream();

        public byte[] getBytes() {
            return bos.toByteArray();
        }

        @Override
        public ByteArrayWriteStream write(Buffer data) {
            try {
                bos.write(data.getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        @Override
        public ByteArrayWriteStream setWriteQueueMaxSize(int maxSize) {
        	return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public ByteArrayWriteStream drainHandler(Handler<Void> handler) {
        	return this;
        }

        @Override
        public ByteArrayWriteStream exceptionHandler(Handler<Throwable> handler) {
        	return this;
        }
    }

    @Override
    public void put(String path, final boolean merge, final long expire, final Handler<Resource> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putArray("args", new JsonArray().add(key + "*"));
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body().getArray("value");
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
                                JsonArray args = new JsonArray();
                                args.add(mergeScript);
                                args.add(1);
                                args.add(key);
                                args.add(encodeBinary(stream.getBytes()));
                                command.putArray("args", args);
                            } else {
                                command.putString("command", "set");
                                command.putArray("args", new JsonArray().add(key).add(encodeBinary(stream.getBytes())));
                            }
                            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                public void handle(Message<JsonObject> event) {
                                    if("error".equals(event.body().getString("status")) && d.errorHandler != null) {
                                        d.errorHandler.handle(event.body().getString("message"));
                                    } else {
                                    	if(expire > 0) {
	                                    	JsonObject command = new JsonObject();
	                                    	command.putString("command", "expire");
	                                    	command.putArray("args", new JsonArray().add(key).add(expire));
	                                    	eb.send(redisAddress, command);              
                                    	}
                                		d.endHandler.handle(null);
                                    }
                                }
                            });
                        }
                    };
                    handler.handle(d);
                } else {
                    CollectionResource c = new CollectionResource();
                    handler.handle(c);
                }
            }
        });
    }

    @Override
    public void delete(String path, final Handler<Resource> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putArray("args", new JsonArray().add(key + "*"));
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body().getArray("value");
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
                command.putArray("args", list);
                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        Resource r = new Resource();
                        handler.handle(r);
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

    private void notFound(Handler<Resource> handler) {
        Resource r = new Resource();
        r.exists = false;
        handler.handle(r);
    }
}
