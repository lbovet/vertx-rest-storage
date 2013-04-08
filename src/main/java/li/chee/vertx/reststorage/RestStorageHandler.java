package li.chee.vertx.reststorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

    MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json; charset=utf-8");

    Map<String, String> editors = new LinkedHashMap<>();
    
    String newMarker = "?new=true";
    
    public RestStorageHandler(final Storage storage, final EtagStore etagStore, final String prefix, JsonObject editorConfig) {

        if(editorConfig != null) {
            for( Entry<String, Object> entry: editorConfig.toMap().entrySet()) {
                editors.put(entry.getKey(), entry.getValue().toString());
            }
        }
        
        routeMatcher.getWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = cleanPath(request.path.substring(prefix.length()));

                String etag = request.headers().get("if-none-match");
                final String resourceEtag = etagStore.get(path);
                if(resourceEtag.equals(etag)) {                    
                    request.response.statusCode = 304;
                    request.response.statusMessage = "Not Modified";
                    request.response.end();
                    return;               
                }
                
                storage.get(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (resource.exists) {
                            String accept = request.headers().get("Accept");
                            boolean html = (accept != null && accept.contains("text/html"));
                            if (resource instanceof CollectionResource) {
                                if (!request.uri.endsWith("/") && !request.uri.contains("/?")) {
                                    request.response.statusCode = 302;
                                    request.response.statusMessage = "Found";
                                    request.response.headers().put("Location", request.uri + "/");
                                    request.response.end();
                                } else {
                                    CollectionResource collection = (CollectionResource) resource;
                                    String collectionName = collectionName(path);
                                    if (html) {                                        
                                        if(!(request.query!=null && request.query.contains("follow=off")) &&
                                                collection.items.size() == 1 &&
                                                collection.items.get(0) instanceof CollectionResource ) {
                                            request.response.statusCode = 302;
                                            request.response.statusMessage = "Found";
                                            request.response.headers().put("Location", (request.uri)+collection.items.get(0).name);
                                            request.response.end();
                                            return;
                                        }
                                        
                                        StringBuilder body = new StringBuilder();
                                        String editor = null;
                                        if(editors.size()>0) {
                                            editor = editors.values().iterator().next();
                                        }
                                        body.append("<!DOCTYPE html>\n");
                                        body.append("<html><head><meta charset='utf-8'/><title>" + collectionName + "</title>");
                                        body.append("<link href='//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.1/css/bootstrap-combined.min.css' rel='stylesheet'></head>");
                                        body.append("<body><div style='font-size: 2em; height:48px; border-bottom: 1px solid lightgray; color: darkgray'><div style='padding:12px;'>" + htmlPath(prefix + path) + "</div>");
                                        if(editor != null) {                                            
                                            String editorString=editor.replace("$path", path+(path.equals("/")? "" : "/")+"$new");
                                            editorString=editorString.replaceFirst("\\?", newMarker);                                            
                                            body.append("<div style='position: fixed; top: 8px; right: 20px;'>" +
                                                    "<input id='name' type='text' placeholder='New Resource\u2026' onkeydown='if (event.keyCode == 13) { if(document.getElementById(\"name\").value) {window.location=\""+editorString+"\".replace(\"$new\",document.getElementById(\"name\").value);}}'></input></div>");
                                        }
                                        body.append("</div><ul style='padding: 12px; font-size: 1.2em;' class='unstyled'><li><a href=\"../?follow=off\">..</a></li>");
                                        List<String> sortedNames = sortedNames(collection);
                                        for (String name : sortedNames) {
                                            body.append("<li><a href=\"" + name + "\">" + name + "</a>");
                                            body.append("</li>");
                                        }
                                        body.append("</ul></body></html>");
                                        request.response.headers().put("Content-Length", body.length());
                                        request.response.headers().put("Content-Type", "text/html; charset=utf-8");
                                        request.response.end(body.toString());
                                    } else {
                                        JsonArray array = new JsonArray();
                                        List<String> sortedNames = sortedNames(collection);
                                        for (String name : sortedNames) {
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
                                    String mimeType = mimeTypeResolver.resolveMimeType(path);
                                    if(request.headers().containsKey("Accept") && request.headers().get("Accept").contains("text/html")) {
                                        String editor = editors.get(mimeType.split(";")[0]);
                                        if(editor != null) {
                                            request.response.statusCode = 302;
                                            request.response.statusMessage = "Found";
                                            String editorString = editor.replaceAll("\\$path", path);
                                            request.response.headers().put("Location", editorString);                                            
                                            request.response.end();
                                            return;
                                        }
                                    }
                                    
                                    final DocumentResource documentResource = (DocumentResource) resource;
                                    if(resourceEtag != "") {
                                        request.response.headers().put("Etag", resourceEtag);
                                    }
                                    request.response.headers().put("Content-Length", documentResource.length);
                                    request.response.headers().put("Content-Type", mimeType);
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
                                collections.add(name + "/");
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
                boolean merge = (request.query != null && request.query.contains("merge=true")
                        && mimeTypeResolver.resolveMimeType(path).contains("application/json"));
                storage.put(path, merge, new Handler<AsyncResult<Resource>>() {
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
                        if (resource instanceof DocumentResource) {
                            request.resume();
                            final DocumentResource documentResource = (DocumentResource) resource;
                            documentResource.errorHandler = new Handler<String>() {
                                public void handle(String error) {                                    
                                    request.response.statusCode=400;
                                    request.response.statusMessage="Bad Request";
                                    request.response.end(error);
                                    etagStore.reset(path);
                                }
                            };
                            documentResource.endHandler = new Handler<Void>() {
                                public void handle(Void event) {
                                    request.response.end();
                                    etagStore.reset(path);
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
                final String path = cleanPath(request.path.substring(prefix.length()));
                storage.delete(path, new Handler<AsyncResult<Resource>>() {
                    public void handle(AsyncResult<Resource> event) {
                        Resource resource = event.result;
                        if (!resource.exists) {
                            request.response.statusCode = 404;
                            request.response.statusMessage = "Not Found";
                            request.response.end("404 Not Found");
                        } else {
                            etagStore.reset(path);
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
        
        routeMatcher.all(".*", new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {
                request.response.statusCode = 405;
                request.response.statusMessage = "Method Not Allowed";
                request.response.end("405 Method Not Allowed");
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
        if (path.equals("/")) {
            return "/";
        }
        StringBuilder sb = new StringBuilder("");
        StringBuilder p = new StringBuilder();
        String[] parts = path.split("/");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            p.append(part);
            p.append("/");
            if (i < parts.length - 1) {
                sb.append(" <a href=\"");
                sb.append(p);
                sb.append("?follow=off\">");
                sb.append(part);
                sb.append("</a> > ");
            } else {
                sb.append(" ");
                sb.append(part);
            }
        }
        return sb.toString();
    }
}
