package li.chee.vertx.reststorage;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    RouteMatcher routeMatcher = new RouteMatcher();

    MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json");

    public RestStorageHandler(final Storage storage, final String prefix) {
        routeMatcher.getWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = cleanPath(request.path.substring(prefix.length()));
                storage.get(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (resource.exists) {
                            if (resource instanceof CollectionResource) {
                                // TODO: redirect if trailing slash missing
                                CollectionResource collection = (CollectionResource) resource;
                                JsonArray array = new JsonArray();
                                for (String s : collection.items) {
                                    array.addString(s);
                                }
                                // TODO: Accept text/html
                                String body = new JsonObject().putArray(collectionName(path), array).encode();
                                request.response.headers().put("Content-Length", body.length());
                                request.response.headers().put("Content-Type", "application/json; charset=utf-8");
                                request.response.end(body);
                            }
                            if (resource instanceof DocumentResource) {
                                // TODO: redirect if trailing slash
                                final DocumentResource documentResource = (DocumentResource) resource;
                                request.response.headers().put("Content-Length", documentResource.length);
                                request.response.headers().put("Content-Type", mimeTypeResolver.resolveMimeType(path));
                                documentResource.readStream.endHandler(new SimpleHandler() {
                                    protected void handle() {
                                        documentResource.closeHandler.handle(null);
                                        request.response.end();
                                    }
                                });
                                // TODO: exception handlers
                                Pump.createPump(documentResource.readStream, request.response).start();
                            }
                        } else {
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end();
                        }
                    }
                });
            }
        });
        routeMatcher.putWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                request.pause();
                final String path = cleanPath(request.path.substring(prefix.length()));
                storage.put(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (!resource.exists) {
                            request.resume();
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end();
                        }
                        if (resource instanceof CollectionResource) {
                            request.resume();
                            request.response.statusCode = 405;
                            request.response.statusMessage = "Method Not Allowed";
                            request.response.headers().put("Allow", "GET, DELETE");
                            request.response.end();
                        }
                        // TODO: merge existing JSON resources
                        if (resource instanceof DocumentResource) {
                            request.resume();
                            final DocumentResource documentResource = (DocumentResource) resource;
                            request.endHandler(new SimpleHandler() {
                                protected void handle() {
                                    System.out.println("closing");
                                    documentResource.closeHandler.handle(null);
                                    request.response.end();
                                }
                            });
                            // TODO: exception handlers
                            Pump.createPump(request, documentResource.writeStream).start();
                        }
                    }
                });
            }
        });
        routeMatcher.deleteWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = cleanPath(request.path.substring(prefix.length()));
                storage.delete(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {     
                        Resource resource = event.result;
                        if (!resource.exists) {
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end();
                        } else {
                            request.response.end();
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

    private String cleanPath(String value) {
        value = value.replaceAll("\\.\\.", "");
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if(value.isEmpty()) {
            return "/";
        }
        return value;
    }

    private String collectionName(String path) {
        if (path.equals("/") && path.equals("")) {
            return "items";
        } else {
            return path.substring(path.lastIndexOf("/") + 1);
        }
    }
}
