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
    private String redisResourcesPrefix;
    private String redisCollectionsPrefix;
    private EventBus eb;
    private Vertx vertx;
    private String mergeScript;
    private String putScript;
    private String getScript;
    private String deleteScript;

    public RedisStorage(Vertx vertx, String redisAddress, String redisResourcesPrefix, String redisCollectionsPrefix) {
        this.redisAddress = redisAddress;
        this.redisResourcesPrefix = redisResourcesPrefix;
        this.redisCollectionsPrefix = redisCollectionsPrefix;
        eb = vertx.eventBus();
        this.vertx = vertx;

        mergeScript = readLuaScript("json-merge.lua");
        putScript = readLuaScript("put.lua");
        getScript = readLuaScript("get.lua");
        deleteScript = readLuaScript("del.lua");
    }

    private String readLuaScript(String script) {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(script)));
        StringBuilder sb;
        try {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                sb.append(line + "\n");
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
        return sb.toString();
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
        command.putString("command", "eval");
        JsonArray args = new JsonArray();
        args.add(getScript);
        args.add(1);
        args.add(key);
        args.add(redisResourcesPrefix);
        args.add(redisCollectionsPrefix);
        command.putArray("args", args);
        System.out.println(command);
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                Object value = event.body().getField("value");
                if ("notFound".equals(value)) {
                    notFound(handler);
                } else if (value instanceof String) {
                    String valueStr = (String) value;
                    DocumentResource r = new DocumentResource();
                    byte[] content = decodeBinary(valueStr);
                    r.readStream = new ByteArrayReadStream(content);
                    r.length = content.length;
                    r.closeHandler = new Handler<Void>() {
                        public void handle(Void event) {
                            // nothing to close
                        }
                    };
                    handler.handle(r);
                } else if (value instanceof JsonArray) {
                    CollectionResource r = new CollectionResource();
                    JsonArray valueList = (JsonArray) value;
                    Set<Resource> items = new HashSet<>();
                    Iterator<Object> iterator = valueList.iterator();
                    while (iterator.hasNext()) {
                        String member = (String) iterator.next();
                        if (member.endsWith(":")) {
                            member = member.replaceAll(":$", "");
                            CollectionResource c = new CollectionResource();
                            c.name = member;
                            items.add(c);
                        } else {
                            DocumentResource d = new DocumentResource();
                            d.name = member;
                            items.add(d);
                        }
                    }
                    r.items = new ArrayList<Resource>(items);
                    Collections.sort(r.items);
                    handler.handle(r);
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
        command.putString("command", "type");
        command.putArray("args", new JsonArray().add(key));
        System.out.println(command);
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                String type = event.body().getString("value");
                if ("set".equals(type)) {
                    CollectionResource c = new CollectionResource();
                    handler.handle(c);
                } else {
                    final DocumentResource d = new DocumentResource();
                    final ByteArrayWriteStream stream = new ByteArrayWriteStream();
                    d.writeStream = stream;
                    d.closeHandler = new Handler<Void>() {
                        public void handle(Void event) {
                            long expireInMillis;
                            if (expire > -1) {
                                expireInMillis = System.currentTimeMillis() + (expire * 1000);
                            } else {
                                // set to very high value = Sat Nov 20 2286 17:46:39
                                expireInMillis = 9999999999999l;
                            }
                            JsonObject command = new JsonObject();
                            command.putString("command", "eval");
                            JsonArray args = new JsonArray();
                            args.add(putScript);
                            args.add(1);
                            args.add(key);
                            args.add(redisResourcesPrefix);
                            args.add(redisCollectionsPrefix);
                            args.add(merge == true ? "true" : "false");
                            args.add(expireInMillis);
                            args.add(encodeBinary(stream.getBytes()));
                            command.putArray("args", args);
                            System.out.println(command);
                            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                public void handle(Message<JsonObject> event) {
                                    if ("error".equals(event.body().getString("status")) && d.errorHandler != null) {
                                        d.errorHandler.handle(event.body().getString("message"));
                                    } else {
                                        if (expire > 0) {
                                            JsonObject command = new JsonObject();
                                            command.putString("command", "expire");
                                            command.putArray("args", new JsonArray().add(key).add(expire));
                                            System.out.println(command);
                                            eb.send(redisAddress, command);
                                        }
                                        d.endHandler.handle(null);
                                    }
                                }
                            });
                        }
                    };
                    handler.handle(d);
                }
            }
        });

    }

    @Override
    public void delete(String path, final Handler<Resource> handler) {
        final String key = encodePath(path);
        JsonObject command = new JsonObject();
        command.putString("command", "eval");
        JsonArray args = new JsonArray();
        args.add(deleteScript);
        args.add(1);
        args.add(key);
        args.add(redisResourcesPrefix);
        args.add(redisCollectionsPrefix);
        command.putArray("args", args);
        System.out.println(command);
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                String result = event.body().getString("value");
                if ("notFound".equals(result)) {
                    notFound(handler);
                    return;
                }
                Resource r = new Resource();
                handler.handle(r);
            }
        });
    }

    private String encodePath(String path) {
        if (path.equals("/")) {
            path = "";
        }
        return path.replaceAll(":", "§").replaceAll("/", ":");
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
