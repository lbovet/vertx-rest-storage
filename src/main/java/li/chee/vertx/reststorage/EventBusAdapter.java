package li.chee.vertx.reststorage;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.vertx.java.core.*;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.*;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetSocket;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides a direct eventbus interface.
 *
 * @author lbovet
 */
public class EventBusAdapter {

    public void init(final Vertx vertx, String address, final Handler<HttpServerRequest> requestHandler) {
        vertx.eventBus().registerHandler(address, new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> message) {
                requestHandler.handle(new MappedHttpServerRequest(vertx, message));
            }
        });
    }

    private class MappedHttpServerRequest implements HttpServerRequest {
        private Vertx vertx;
        private Buffer requestPayload;
        private String method;
        private String uri;
        private String path;
        private String query;
        private MultiMap params;
        private MultiMap requestHeaders;
        private Message<Buffer> message;
        private Handler<Buffer> dataHandler;
        private Handler<Void> endHandler;
        private HttpServerResponse response;

        private MappedHttpServerRequest(Vertx vertx, Message<Buffer> message) {
            this.vertx = vertx;
            this.message = message;
            Buffer buffer = message.body();
            int headerLength =  buffer.getInt(0);
            JsonObject header = new JsonObject(buffer.getString(4,headerLength+4));
            method = header.getString("method");
            uri = header.getString("uri");
            requestPayload = buffer.getBuffer(headerLength+4, buffer.length());

            JsonArray headerArray = header.getArray("headers");
            if(headerArray != null) {
                requestHeaders = fromJson(headerArray);
            } else {
                requestHeaders = new CaseInsensitiveMultiMap();
            }
        }

        @Override
        public HttpVersion version() {
            return HttpVersion.HTTP_1_0;
        }

        @Override
        public String method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public String path() {
            if(path==null) {
                path = UrlParser.path(uri);
            }
            return path;
        }

        @Override
        public String query() {
            if(query==null) {
                query = UrlParser.query(uri);
            }
            return query;
        }

        @Override
        public HttpServerResponse response() {
            if(response == null) {
                response = new HttpServerResponse() {

                    private int statusCode;
                    private String statusMessage;
                    private MultiMap responseHeaders = new CaseInsensitiveMultiMap();
                    private Buffer responsePayload = new Buffer();

                    @Override
                    public int getStatusCode() {
                        return statusCode;
                    }

                    @Override
                    public HttpServerResponse setStatusCode(int i) {
                        statusCode = i;
                        return this;
                    }

                    @Override
                    public String getStatusMessage() {
                        return statusMessage;
                    }

                    @Override
                    public HttpServerResponse setStatusMessage(String s) {
                        statusMessage = s;
                        return this;
                    }

                    @Override
                    public HttpServerResponse setChunked(boolean b) {
                        return this;
                    }

                    @Override
                    public boolean isChunked() {
                        return false;
                    }

                    @Override
                    public MultiMap headers() {
                        return responseHeaders;
                    }

                    @Override
                    public HttpServerResponse putHeader(String s, String s2) {
                        responseHeaders.set(s, s2);
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(CharSequence charSequence, CharSequence charSequence2) {
                        responseHeaders.set(charSequence, charSequence2);
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(String s, Iterable<String> strings) {
                        for (String value : strings) {
                            responseHeaders.add(s, value);
                        }
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(CharSequence charSequence, Iterable<CharSequence> charSequences) {
                        for (CharSequence value : charSequences) {
                            responseHeaders.add(charSequence, value);
                        }
                        return this;
                    }

                    @Override
                    public MultiMap trailers() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(String s, String s2) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(CharSequence charSequence, CharSequence charSequence2) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(String s, Iterable<String> strings) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(CharSequence charSequence, Iterable<CharSequence> charSequences) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse closeHandler(Handler<Void> voidHandler) {
                        return this;
                    }

                    @Override
                    public HttpServerResponse write(Buffer buffer) {
                        responsePayload.appendBuffer(buffer);
                        return this;
                    }

                    @Override
                    public HttpServerResponse write(String s, String s2) {
                        responsePayload.appendBuffer(new Buffer(s, s2));
                        return this;
                    }

                    @Override
                    public HttpServerResponse write(String s) {
                        responsePayload.appendBuffer(new Buffer(s));
                        return this;
                    }

                    @Override
                    public void end(String s) {
                        write(new Buffer(s));
                        end();
                    }

                    @Override
                    public void end(String s, String s2) {
                        write(s, s2);
                        end();
                    }

                    @Override
                    public void end(Buffer buffer) {
                        write(buffer);
                        end();
                    }

                    @Override
                    public void end() {
                        JsonObject header = new JsonObject();
                        if (statusCode == 0) {
                            statusCode = 200;
                            statusMessage = "OK";
                        }
                        header.putNumber("statusCode", statusCode);
                        header.putString("statusMessage", statusMessage);
                        header.putArray("headers", toJson(responseHeaders));
                        Buffer bufferHeader = new Buffer(header.encode());
                        Buffer response = new Buffer(4+bufferHeader.length()+responsePayload.length());
                        response.setInt(0, bufferHeader.length()).appendBuffer(bufferHeader).appendBuffer(responsePayload);
                        message.reply(response);
                    }

                    @Override
                    public HttpServerResponse sendFile(String s) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse sendFile(String s, String s2) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse sendFile(String s, Handler<AsyncResult<Void>> asyncResultHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse sendFile(String s, String s2, Handler<AsyncResult<Void>> asyncResultHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public HttpServerResponse setWriteQueueMaxSize(int i) {
                        return this;
                    }

                    @Override
                    public boolean writeQueueFull() {
                        return false;
                    }

                    @Override
                    public HttpServerResponse drainHandler(Handler<Void> voidHandler) {
                        return this;
                    }

                    @Override
                    public HttpServerResponse exceptionHandler(Handler<Throwable> throwableHandler) {
                        return this;
                    }
                };
            }
            return response;
        }

        @Override
        public MultiMap headers() {
            return requestHeaders;
        }

        @Override
        public MultiMap params() {
            if (params == null) {
                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri());
                Map<String, List<String>> prms = queryStringDecoder.parameters();
                params = new CaseInsensitiveMultiMap();
                if (!prms.isEmpty()) {
                    for (Map.Entry<String, List<String>> entry: prms.entrySet()) {
                        params.add(entry.getKey(), entry.getValue());
                    }
                }
            }
            return params;
        }

        @Override
        public InetSocketAddress remoteAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InetSocketAddress localAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public X509Certificate[] peerCertificateChain() throws SSLPeerUnverifiedException {
            return new X509Certificate[0];
        }

        @Override
        public URI absoluteURI() {
            return null;
        }

        @Override
        public HttpServerRequest bodyHandler(final Handler<Buffer> bodyHandler) {
            final Buffer body = new Buffer();
            dataHandler(new Handler<Buffer>() {
                public void handle(Buffer buff) {
                    body.appendBuffer(buff);
                }
            });
            endHandler(new VoidHandler() {
                public void handle() {
                    bodyHandler.handle(body);
                }
            });
            return this;
        }

        @Override
        public NetSocket netSocket() {
            return null;
        }

        @Override
        public HttpServerRequest expectMultiPart(boolean b) {
            return this;
        }

        @Override
        public HttpServerRequest uploadHandler(Handler<HttpServerFileUpload> httpServerFileUploadHandler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiMap formAttributes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest endHandler(Handler<Void> voidHandler) {
            endHandler = voidHandler;
            if(requestPayload == null) {
                endHandler.handle(null);
            }
            return this;
        }

        @Override
        public HttpServerRequest dataHandler(Handler<Buffer> bufferHandler) {
            if(requestPayload != null) {
                dataHandler = bufferHandler;
                vertx.runOnContext(new Handler<Void>() {
                    @Override
                    public void handle(Void aVoid) {
                        dataHandler.handle(requestPayload);
                        endHandler.handle(null);
                    }
                });
            }
            return this;
        }

        @Override
        public HttpServerRequest pause() {
            return this;
        }

        @Override
        public HttpServerRequest resume() {
            return this;
        }

        @Override
        public HttpServerRequest exceptionHandler(Handler<Throwable> throwableHandler) {
            return this;
        }
    }

    public static JsonArray toJson(MultiMap multiMap) {
        JsonArray result = new JsonArray();
        for(Map.Entry<String, String> entry: multiMap.entries()) {
            result.addArray(new JsonArray().add(entry.getKey()).add(entry.getValue()));
        }
        return result;
    }

    public static MultiMap fromJson(JsonArray json) {
        MultiMap result = new CaseInsensitiveMultiMap();
        Iterator<Object> it = json.iterator();
        while(it.hasNext()) {
            Object next = it.next();
            if(next instanceof JsonArray) {
                JsonArray pair = (JsonArray)next;
                if(pair.size() == 2) {
                    result.add(pair.get(0).toString(), pair.get(1).toString());
                }
            }
        }
        return result;
    }
}
