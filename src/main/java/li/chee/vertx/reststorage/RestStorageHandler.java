package li.chee.vertx.reststorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.streams.Pump;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    public static final String EXPIRE_AFTER_HEADER = "x-expire-after";
    public static final String ETAG_HEADER = "Etag";
    public static final String IF_NONE_MATCH_HEADER = "if-none-match";
    
    private static final String OFFSET_PARAMETER = "offset";
    private static final String LIMIT_PARAMETER = "limit";
    private static final String BULK_EXPAND_PARAMETER = "bulkExpand";

    Logger log;

    RouteMatcher routeMatcher = new RouteMatcher();

    MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json; charset=utf-8");

    Map<String, String> editors = new LinkedHashMap<>();

    String newMarker = "?new=true";

    public RestStorageHandler(final Logger log, final Storage storage, final String prefix, JsonObject editorConfig) {

        this.log = log;
        
        if (editorConfig != null) {
            for (Entry<String, Object> entry : editorConfig.toMap().entrySet()) {
                editors.put(entry.getKey(), entry.getValue().toString());
            }
        }

        routeMatcher.postWithRegEx(".*_cleanup", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                if(log.isTraceEnabled()) {
                    log.trace("RestStorageHandler cleanup");
                }
                storage.cleanup(new Handler<DocumentResource>() {
                    public void handle(final DocumentResource documentResource) {
                        if(log.isTraceEnabled()) {
                            log.trace("RestStorageHandler cleanup");
                        }
                        request.response().headers().add("Content-Length", "" + documentResource.length);
                        request.response().headers().add("Content-Type", "application/json; charset=utf-8");
                        request.response().setStatusCode(200);
                        final Pump pump = Pump.createPump(documentResource.readStream, request.response());
                        documentResource.readStream.endHandler(new Handler<Void>() {
                            public void handle(Void nothing) {
                                documentResource.closeHandler.handle(null);
                                request.response().end();
                            }
                        });
                        pump.start();
                    }
                }, request.params().get("cleanupResourcesAmount"));
            }
        });

        routeMatcher.postWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                if (!request.params().contains(BULK_EXPAND_PARAMETER)) {
                    respondWithNotAllowed(request);
                } else {
                    request.bodyHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer event) {
                            JsonObject body = new JsonObject(event.toString());
                            JsonArray subResourcesArray = body.getArray("subResources");
                            if (subResourcesArray == null) {
                                request.response().setStatusCode(400);
                                request.response().setStatusMessage("Bad Request");
                                request.response().end("Bad Request: Expected array field 'subResources' with names of resources");
                                return;
                            }

                            List<String> subResourceNames = new ArrayList<String>();
                            for (int i = 0; i < subResourcesArray.size(); i++) {
                                 subResourceNames.add((String) subResourcesArray.get(i));
                            }

                            final String path = cleanPath(request.path().substring(prefix.length()));
                            final String etag = request.headers().get(IF_NONE_MATCH_HEADER);
                            storage.bulkExpand(path, etag, subResourceNames, new Handler<Resource>() {
                                @Override
                                public void handle(Resource resource) {

                                    if (!resource.modified) {
                                        request.response().setStatusCode(304);
                                        request.response().setStatusMessage("Not Modified");
                                        request.response().headers().set(ETAG_HEADER, etag);
                                        request.response().headers().add("Content-Length", "0");
                                        request.response().end();
                                        return;
                                    }

                                    if (resource.exists) {
                                        if (log.isTraceEnabled()) {
                                            log.trace("RestStorageHandler resource is a DocumentResource: " + request.uri());
                                        }

                                        String mimeType = mimeTypeResolver.resolveMimeType(path);
                                        final DocumentResource documentResource = (DocumentResource) resource;
                                        if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                                            request.response().headers().add(ETAG_HEADER, documentResource.etag);
                                        }
                                        request.response().headers().add("Content-Length", "" + documentResource.length);
                                        request.response().headers().add("Content-Type", mimeType);
                                        final Pump pump = Pump.createPump(documentResource.readStream, request.response());
                                        documentResource.readStream.endHandler(new Handler<Void>() {
                                            public void handle(Void nothing) {
                                                documentResource.closeHandler.handle(null);
                                                request.response().end();
                                            }
                                        });
                                        pump.start();
                                        // TODO: exception handlers

                                    } else {
                                        if (log.isTraceEnabled()) {
                                            log.trace("RestStorageHandler Could not find resource: " + request.uri());
                                        }
                                        request.response().setStatusCode(404);
                                        request.response().setStatusMessage("Not Found");
                                        request.response().end("404 Not Found");
                                    }
                                }
                            });
                        }
                    });
                }
            }
        });

        routeMatcher.getWithRegEx(prefix + ".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String path = cleanPath(request.path().substring(prefix.length()));
                final String etag = request.headers().get(IF_NONE_MATCH_HEADER);
                if(log.isTraceEnabled()) {
                    log.trace("RestStorageHandler got GET Request path: " + path + " etag: " + etag);
                }
                String offsetFromUrl = request.params().get(OFFSET_PARAMETER);
                String limitFromUrl = request.params().get(LIMIT_PARAMETER);
                OffsetLimit offsetLimit = UrlParser.offsetLimit(offsetFromUrl, limitFromUrl);
                storage.get(path, etag, offsetLimit.offset, offsetLimit.limit, new Handler<Resource>() {
                    public void handle(Resource resource) {
                        if(log.isTraceEnabled()) {
                            log.trace("RestStorageHandler resource exists: " + resource.exists);
                        }

                        if(!resource.modified){
                            request.response().setStatusCode(304);
                            request.response().setStatusMessage("Not Modified");
                            request.response().headers().set(ETAG_HEADER, etag);
                            request.response().headers().add("Content-Length", "0");
                            request.response().end();
                            return;
                        }

                        if (resource.exists) {
                            String accept = request.headers().get("Accept");
                            boolean html = (accept != null && accept.contains("text/html"));
                            if (resource instanceof CollectionResource) {
                                if(log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler resource is collection: " + request.uri());
                                }
                                CollectionResource collection = (CollectionResource) resource;
                                String collectionName = collectionName(path);
                                if (html && !request.uri().endsWith("/")) {
                                    if(log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler accept contains text/html and ends with /");
                                    }
                                    request.response().setStatusCode(302);
                                    request.response().setStatusMessage("Found");
                                    request.response().headers().add("Location", request.uri() + "/");
                                    request.response().end();
                                } else if(html) {
                                    if(log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler accept contains text/html");
                                    }
                                    if (!(request.query() != null && request.query().contains("follow=off")) &&
                                            collection.items.size() == 1 &&
                                            collection.items.get(0) instanceof CollectionResource) {
                                        if(log.isTraceEnabled()) {
                                            log.trace("RestStorageHandler query contains follow=off");
                                        }
                                        request.response().setStatusCode(302);
                                        request.response().setStatusMessage("Found");
                                        request.response().headers().add("Location", (request.uri()) + collection.items.get(0).name);
                                        request.response().end();
                                        return;
                                    }

                                    StringBuilder body = new StringBuilder();
                                    String editor = null;
                                    if (editors.size() > 0) {
                                        editor = editors.values().iterator().next();
                                    }
                                    body.append("<!DOCTYPE html>\n");
                                    body.append("<html><head><meta charset='utf-8'/><title>" + collectionName + "</title>");
                                    body.append("<link href='//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.1/css/bootstrap-combined.min.css' rel='stylesheet'></head>");
                                    body.append("<body><div style='font-size: 2em; height:48px; border-bottom: 1px solid lightgray; color: darkgray'><div style='padding:12px;'>" + htmlPath(prefix + path) + "</div>");
                                    if (editor != null) {
                                        String editorString = editor.replace("$path", path + (path.equals("/") ? "" : "/") + "$new");
                                        editorString = editorString.replaceFirst("\\?", newMarker);
                                        body.append("<div style='position: fixed; top: 8px; right: 20px;'>" +
                                                "<input id='name' type='text' placeholder='New Resource\u2026' onkeydown='if (event.keyCode == 13) { if(document.getElementById(\"name\").value) {window.location=\"" + editorString + "\".replace(\"$new\",document.getElementById(\"name\").value);}}'></input></div>");
                                    }
                                    body.append("</div><ul style='padding: 12px; font-size: 1.2em;' class='unstyled'><li><a href=\"../?follow=off\">..</a></li>");
                                    List<String> sortedNames = sortedNames(collection);
                                    for (String name : sortedNames) {
                                        body.append("<li><a href=\"" + name + "\">" + name + "</a>");
                                        body.append("</li>");
                                    }
                                    body.append("</ul></body></html>");
                                    request.response().headers().add("Content-Length", "" + body.length());
                                    request.response().headers().add("Content-Type", "text/html; charset=utf-8");
                                    request.response().end(body.toString());
                                } else {
                                    JsonArray array = new JsonArray();
                                    List<String> sortedNames = sortedNames(collection);
                                    for (String name : sortedNames) {
                                        array.addString(name);
                                    }
                                    if(log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler return collection: " + sortedNames);
                                    }
                                    String body = new JsonObject().putArray(collectionName, array).encode();
                                    request.response().headers().add("Content-Length", "" + body.length());
                                    request.response().headers().add("Content-Type", "application/json; charset=utf-8");
                                    request.response().end(body);
                                }
                            }
                            if (resource instanceof DocumentResource) {
                                if(log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler resource is a DocumentResource: " + request.uri());
                                }
                                if (request.uri().endsWith("/")) {
                                    if(log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler DocumentResource ends with /");
                                    }
                                    request.response().setStatusCode(302);
                                    request.response().setStatusMessage("Found");
                                    request.response().headers().add("Location", request.uri().substring(0, request.uri().length() - 1));
                                    request.response().end();
                                } else {
                                    if(log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler DocumentResource does not end with /");
                                    }
                                    String mimeType = mimeTypeResolver.resolveMimeType(path);
                                    if (request.headers().names().contains("Accept") && request.headers().get("Accept").contains("text/html")) {
                                        String editor = editors.get(mimeType.split(";")[0]);
                                        if (editor != null) {
                                            request.response().setStatusCode(302);
                                            request.response().setStatusMessage("Found");
                                            String editorString = editor.replaceAll("\\$path", path);
                                            request.response().headers().add("Location", editorString);
                                            request.response().end();
                                            return;
                                        }
                                    }

                                    final DocumentResource documentResource = (DocumentResource) resource;
                                    if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                                        request.response().headers().add(ETAG_HEADER, documentResource.etag);
                                    }
                                    request.response().headers().add("Content-Length", "" + documentResource.length);
                                    request.response().headers().add("Content-Type", mimeType);
                                    final Pump pump = Pump.createPump(documentResource.readStream, request.response());
                                    documentResource.readStream.endHandler(new Handler<Void>() {
                                        public void handle(Void nothing) {
                                            documentResource.closeHandler.handle(null);
                                            request.response().end();
                                        }
                                    });
                                    pump.start();
                                    // TODO: exception handlers
                                }
                            }
                        } else {
                            if(log.isTraceEnabled()) {
                                log.trace("RestStorageHandler Could not find resource: " + request.uri());
                            }
                            request.response().setStatusCode(404);
                            request.response().setStatusMessage("Not Found");
                            request.response().end("404 Not Found");
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
                final String path = cleanPath(request.path().substring(prefix.length()));
                long expire = -1;
                if (request.headers().contains(EXPIRE_AFTER_HEADER)) {
                    try {
                        expire = Long.parseLong(request.headers().get(EXPIRE_AFTER_HEADER));
                    } catch (NumberFormatException nfe) {
                        request.resume();
                        request.response().setStatusCode(400);
                        request.response().setStatusMessage("Invalid expire after parameter: " + request.headers().get(EXPIRE_AFTER_HEADER));
                        request.response().end(request.response().getStatusMessage());
                        log.error("Expire after header, invalid value: " + request.response().getStatusMessage());
                        return;
                    }
                }
                boolean merge = (request.query() != null && request.query().contains("merge=true")
                        && mimeTypeResolver.resolveMimeType(path).contains("application/json"));
                if(log.isTraceEnabled()) {
                    log.trace("RestStorageHandler put resource: " + request.uri() + " with expire: " + expire);
                }

                final String etag = request.headers().get(IF_NONE_MATCH_HEADER);

                storage.put(path, etag, merge, expire, new Handler<Resource>() {
                    public void handle(Resource resource) {
                        request.resume();

                        if(!resource.modified){
                            request.response().setStatusCode(304);
                            request.response().setStatusMessage("Not Modified");
                            request.response().headers().set(ETAG_HEADER, etag);
                            request.response().headers().add("Content-Length", "0");
                            request.response().end();
                            return;
                        }

                        if (!resource.exists && resource instanceof DocumentResource) {
                            request.response().setStatusCode(405);
                            request.response().setStatusMessage("Method Not Allowed");
                            request.response().headers().add("Allow", "GET, DELETE");
                            request.response().end();
                        }
                        if (resource instanceof CollectionResource) {
                            request.response().setStatusCode(405);
                            request.response().setStatusMessage("Method Not Allowed");
                            request.response().headers().add("Allow", "GET, DELETE");
                            request.response().end();
                        }
                        if (resource instanceof DocumentResource) {
                            final DocumentResource documentResource = (DocumentResource) resource;
                            documentResource.errorHandler = new Handler<String>() {
                                public void handle(String error) {
                                    request.response().setStatusCode(400);
                                    request.response().setStatusMessage("Bad Request");
                                    request.response().end(error);
                                }
                            };
                            documentResource.endHandler = new Handler<Void>() {
                                public void handle(Void event) {
                                    request.response().end();
                                }
                            };
                            final Pump pump = Pump.createPump(request, documentResource.writeStream);
                            request.endHandler(new Handler<Void>() {
                                public void handle(Void v) {
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
                final String path = cleanPath(request.path().substring(prefix.length()));
                if(log.isTraceEnabled()) {
                    log.trace("RestStorageHandler delete resource: " + request.uri());
                }
                storage.delete(path, new Handler<Resource>() {
                    public void handle(Resource resource) {
                        if (!resource.exists) {
                            request.response().setStatusCode(404);
                            request.response().setStatusMessage("Not Found");
                            request.response().end("404 Not Found");
                        } else {
                            request.response().end();
                        }
                    }
                });
            }
        });

        routeMatcher.get(".*", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                if(log.isTraceEnabled()) {
                    log.trace("RestStorageHandler resource not found: " + request.uri());
                }
                request.response().setStatusCode(404);
                request.response().setStatusMessage("Not Found");
                request.response().end("404 Not Found");
            }
        });

        routeMatcher.all(".*", new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {
                respondWithNotAllowed(request);
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
    
    public static class OffsetLimit {
    	public OffsetLimit(int offset, int limit) {
			this.offset = offset;
			this.limit = limit;
		}
		public int offset;
    	public int limit;
    }

    private void respondWithNotAllowed(HttpServerRequest request){
        request.response().setStatusCode(405);
        request.response().setStatusMessage("Method Not Allowed");
        request.response().end("405 Method Not Allowed");
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
