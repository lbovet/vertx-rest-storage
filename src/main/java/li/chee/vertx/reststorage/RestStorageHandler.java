package li.chee.vertx.reststorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.deploy.impl.VertxLocator;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    RouteMatcher routeMatcher = new RouteMatcher();

    MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json; charset=utf-8");

    public RestStorageHandler(final Storage storage, final String prefix) {
        routeMatcher.getWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = cleanPath(request.path.substring(prefix.length()));
                storage.get(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (resource.exists) {
                            String accept = request.headers().get("Accept");
                            boolean html = (accept != null && accept.contains("text/html"));
                            if (resource instanceof CollectionResource) {
                                if (!request.uri.endsWith("/")) {
                                    request.response.statusCode = 302;
                                    request.response.statusMessage = "Found";
                                    request.response.headers().put("Location", request.uri + "/");
                                    request.response.end();
                                } else {
                                    CollectionResource collection = (CollectionResource) resource;
                                    String collectionName = collectionName(path);
                                    if (html) {
                                        StringBuilder body = new StringBuilder();
                                        body.append("<!DOCTYPE html>\n");
                                        body.append("<html><head><title>" + collectionName + "</title></head>");
                                        body.append("<body><h1>" + htmlPath(prefix+path) + "</h1><ul><li><a href=\"..\">..</a></li>");
                                        List<String> sortedNames = sortedNames(collection);
                                        for(String name: sortedNames) {
                                            body.append("<li><a href=\"" + name + "\">" + name + "</a></li>");
                                        }
                                        body.append("</ul></body></html>");
                                        request.response.headers().put("Content-Length", body.length());
                                        request.response.headers().put("Content-Type", "text/html; charset=utf-8");
                                        request.response.end(body.toString());
                                    } else {
                                        JsonArray array = new JsonArray();
                                        List<String> sortedNames = sortedNames(collection);
                                        for(String name: sortedNames) {
                                            array.addString(name);
                                        }
                                        String body = new JsonObject().putArray(collectionName, array).encode();
                                        request.response.headers().put("Content-Length", body.length());
                                        request.response.headers().put("Content-Type", "application/json; charset=utf-8");
                                        request.response.end(body);
                                    }
                                }
                            }
                            if (resource instanceof DocumentResource) {
                                if (request.uri.endsWith("/")) {
                                    request.response.statusCode = 302;
                                    request.response.statusMessage = "Found";
                                    request.response.headers().put("Location", request.uri.substring(0, request.uri.length() - 1));
                                    request.response.end();
                                } else {
                                    final DocumentResource documentResource = (DocumentResource) resource;
                                    request.response.headers().put("Content-Length", documentResource.length);
                                    request.response.headers().put("Content-Type", mimeTypeResolver.resolveMimeType(path));
                                    final Pump pump = Pump.createPump(documentResource.readStream, request.response);                                    
                                    documentResource.readStream.endHandler(new SimpleHandler() {
                                        protected void handle() {
                                            documentResource.closeHandler.handle(null);
                                            request.response.end();
                                        }
                                    });
                                    pump.start();
                                    // TODO: exception handlers
                                }
                            }
                        } else {
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end("404 Not Found");
                        }
                    }

                    private List<String> sortedNames(CollectionResource collection) {
                        List<String> collections = new ArrayList<>();
                        List<String> documents = new ArrayList<>();
                        for (Resource r : collection.items) {
                            String name = r.name;
                            if (r instanceof CollectionResource) {
                                collections.add(name+"/");
                            } else {
                                documents.add(name);
                            }                            
                        }
                        collections.addAll(documents);
                        return collections;
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
                            request.response.end("404 Not Found");
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
                            documentResource.endHandler = new Handler<Void>() {
                                public void handle(Void event) {
                                    request.response.end();
                                }
                            };                            
                            final Pump pump = Pump.createPump(request, documentResource.writeStream);
                            request.endHandler(new SimpleHandler() {
                                protected void handle() {
                                    documentResource.closeHandler.handle(null);                                    
                                }
                            });
                            // TODO: exception handlers
                            pump.start();
                        }
                    }
                });
            }
        });
        routeMatcher.deleteWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                System.out.println("delete");
                final String path = cleanPath(request.path.substring(prefix.length()));                
                storage.delete(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (!resource.exists) {
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end("404 Not Found");
                        } else {
                            request.response.end();
                        }
                    }
                });
            }
        });
        routeMatcher.get(".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                request.response.statusCode = 404;
                request.response.statusMessage = "Not Found";
                request.response.end("404 Not Found");
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
        if (value.isEmpty()) {
            return "/";
        }
        return value;
    }

    private String collectionName(String path) {
        if (path.equals("/") || path.equals("")) {
            return "root";
        } else {
            return path.substring(path.lastIndexOf("/") + 1);
        }
    }
    
    private String htmlPath(String path) {
        if(path.equals("/")) {
            return "/";
        }
        StringBuilder sb = new StringBuilder("");
        StringBuilder p = new StringBuilder();
        String[] parts = path.split("/");
        for(int i=0; i<parts.length; i++) {
            String part = parts[i];
            p.append(part);
            p.append("/");
            if(i < parts.length-1) {
                sb.append(" <a href=\"");            
                sb.append(p);
                sb.append("\">");
                sb.append(part);
                sb.append("/</a>");
            } else {
                sb.append(" ");
                sb.append(part);
            }
        }
        return sb.toString();
    }
}
