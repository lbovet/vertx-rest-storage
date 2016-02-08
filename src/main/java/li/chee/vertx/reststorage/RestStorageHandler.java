package li.chee.vertx.reststorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.Pump;
import io.vertx.ext.web.Router;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    public static final String EXPIRE_AFTER_HEADER = "x-expire-after";
    public static final String ETAG_HEADER = "Etag";
    public static final String IF_NONE_MATCH_HEADER = "if-none-match";

    private static final String OFFSET_PARAMETER = "offset";
    private static final String LIMIT_PARAMETER = "limit";
    private static final String BULK_EXPAND_PARAMETER = "bulkExpand";

    Logger log;
    Router router;

    MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json; charset=utf-8");

    Map<String, String> editors = new LinkedHashMap<>();

    String newMarker = "?new=true";

    public RestStorageHandler(Vertx vertx, final Logger log, final Storage storage, final String prefix, JsonObject editorConfig) {
        this.router = Router.router(vertx);
        this.log = log;

        if (editorConfig != null) {
            for (Entry<String, Object> entry : editorConfig.getMap().entrySet()) {
                editors.put(entry.getKey(), entry.getValue().toString());
            }
        }

        router.postWithRegex(".*_cleanup").handler(ctx -> {
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler cleanup");
            }
            storage.cleanup(documentResource -> {
                if (log.isTraceEnabled()) {
                    log.trace("RestStorageHandler cleanup");
                }
                ctx.response().headers().add("Content-Length", "" + documentResource.length);
                ctx.response().headers().add("Content-Type", "application/json; charset=utf-8");
                ctx.response().setStatusCode(200);
                final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
                documentResource.readStream.endHandler(nothing -> {
                    documentResource.closeHandler.handle(null);
                    ctx.response().end();
                });
                pump.start();
            }, ctx.request().params().get("cleanupResourcesAmount"));
        });

        router.postWithRegex(prefix + ".*").handler(ctx -> {
            if (!ctx.request().params().contains(BULK_EXPAND_PARAMETER)) {
                respondWithNotAllowed(ctx.request());
            } else {
                ctx.request().bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer event) {
                        List<String> subResourceNames = new ArrayList<>();
                        try {
                            JsonObject body = new JsonObject(event.toString());
                            JsonArray subResourcesArray = body.getJsonArray("subResources");
                            if (subResourcesArray == null) {
                                respondWithBadRequest(ctx.request(), "Bad Request: Expected array field 'subResources' with names of resources");
                                return;
                            }

                            for (int i = 0; i < subResourcesArray.size(); i++) {
                                subResourceNames.add(subResourcesArray.getString(i));
                            }
                        } catch(RuntimeException ex){
                            respondWithBadRequest(ctx.request(), "Bad Request: Unable to parse body of bulkExpand POST request");
                            return;
                        }

                        final String path = cleanPath(ctx.request().path().substring(prefix.length()));
                        final String etag = ctx.request().headers().get(IF_NONE_MATCH_HEADER);
                        storage.bulkExpand(path, etag, subResourceNames, resource -> {

                            if (!resource.modified) {
                                ctx.response().setStatusCode(304);
                                ctx.response().setStatusMessage("Not Modified");
                                ctx.response().headers().set(ETAG_HEADER, etag);
                                ctx.response().headers().add("Content-Length", "0");
                                ctx.response().end();
                                return;
                            }

                            if (resource.exists) {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler resource is a DocumentResource: " + ctx.request().uri());
                                }

                                String mimeType = mimeTypeResolver.resolveMimeType(path);
                                final DocumentResource documentResource = (DocumentResource) resource;
                                if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                                    ctx.response().headers().add(ETAG_HEADER, documentResource.etag);
                                }
                                ctx.response().headers().add("Content-Length", "" + documentResource.length);
                                ctx.response().headers().add("Content-Type", mimeType);
                                final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
                                documentResource.readStream.endHandler(nothing -> {
                                    documentResource.closeHandler.handle(null);
                                    ctx.response().end();
                                });
                                pump.start();
                                // TODO: exception handlers

                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler Could not find resource: " + ctx.request().uri());
                                }
                                ctx.response().setStatusCode(404);
                                ctx.response().setStatusMessage("Not Found");
                                ctx.response().end("404 Not Found");
                            }
                        });
                    }
                });
            }

        });

        router.getWithRegex(prefix + ".*").handler(ctx -> {
            final String path = cleanPath(ctx.request().path().substring(prefix.length()));
            final String etag = ctx.request().headers().get(IF_NONE_MATCH_HEADER);
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler got GET Request path: " + path + " etag: " + etag);
            }
            String offsetFromUrl = ctx.request().params().get(OFFSET_PARAMETER);
            String limitFromUrl = ctx.request().params().get(LIMIT_PARAMETER);
            OffsetLimit offsetLimit = UrlParser.offsetLimit(offsetFromUrl, limitFromUrl);
            storage.get(path, etag, offsetLimit.offset, offsetLimit.limit, new Handler<Resource>() {
                public void handle(Resource resource) {
                    if (log.isTraceEnabled()) {
                        log.trace("RestStorageHandler resource exists: " + resource.exists);
                    }

                    if (!resource.modified) {
                        ctx.response().setStatusCode(304);
                        ctx.response().setStatusMessage("Not Modified");
                        ctx.response().headers().set(ETAG_HEADER, etag);
                        ctx.response().headers().add("Content-Length", "0");
                        ctx.response().end();
                        return;
                    }

                    if (resource.exists) {
                        String accept = ctx.request().headers().get("Accept");
                        boolean html = (accept != null && accept.contains("text/html"));
                        if (resource instanceof CollectionResource) {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler resource is collection: " + ctx.request().uri());
                            }
                            CollectionResource collection = (CollectionResource) resource;
                            String collectionName = collectionName(path);
                            if (html && !ctx.request().uri().endsWith("/")) {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler accept contains text/html and ends with /");
                                }
                                ctx.response().setStatusCode(302);
                                ctx.response().setStatusMessage("Found");
                                ctx.response().headers().add("Location", ctx.request().uri() + "/");
                                ctx.response().end();
                            } else if (html) {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler accept contains text/html");
                                }
                                if (!(ctx.request().query() != null && ctx.request().query().contains("follow=off")) &&
                                        collection.items.size() == 1 &&
                                        collection.items.get(0) instanceof CollectionResource) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("RestStorageHandler query contains follow=off");
                                    }
                                    ctx.response().setStatusCode(302);
                                    ctx.response().setStatusMessage("Found");
                                    ctx.response().headers().add("Location", (ctx.request().uri()) + collection.items.get(0).name);
                                    ctx.response().end();
                                    return;
                                }

                                StringBuilder body = new StringBuilder();
                                String editor = null;
                                if (editors.size() > 0) {
                                    editor = editors.values().iterator().next();
                                }
                                body.append("<!DOCTYPE html>\n");
                                body.append("<html><head><meta charset='utf-8'/><title>").append(collectionName).append("</title>");
                                body.append("<link href='//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.1/css/bootstrap-combined.min.css' rel='stylesheet'></head>");
                                body.append("<body><div style='font-size: 2em; height:48px; border-bottom: 1px solid lightgray; color: darkgray'><div style='padding:12px;'>").append(htmlPath(prefix + path)).append("</div>");
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
                                ctx.response().headers().add("Content-Length", "" + body.length());
                                ctx.response().headers().add("Content-Type", "text/html; charset=utf-8");
                                ctx.response().end(body.toString());
                            } else {
                                JsonArray array = new JsonArray();
                                List<String> sortedNames = sortedNames(collection);
                                sortedNames.forEach(array::add);
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler return collection: " + sortedNames);
                                }
                                String body = new JsonObject().put(collectionName, array).encode();
                                ctx.response().headers().add("Content-Length", "" + body.length());
                                ctx.response().headers().add("Content-Type", "application/json; charset=utf-8");
                                ctx.response().end(body);
                            }
                        }
                        if (resource instanceof DocumentResource) {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler resource is a DocumentResource: " + ctx.request().uri());
                            }
                            if (ctx.request().uri().endsWith("/")) {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler DocumentResource ends with /");
                                }
                                ctx.response().setStatusCode(302);
                                ctx.response().setStatusMessage("Found");
                                ctx.response().headers().add("Location", ctx.request().uri().substring(0, ctx.request().uri().length() - 1));
                                ctx.response().end();
                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler DocumentResource does not end with /");
                                }
                                String mimeType = mimeTypeResolver.resolveMimeType(path);
                                if (ctx.request().headers().names().contains("Accept") && ctx.request().headers().get("Accept").contains("text/html")) {
                                    String editor = editors.get(mimeType.split(";")[0]);
                                    if (editor != null) {
                                        ctx.response().setStatusCode(302);
                                        ctx.response().setStatusMessage("Found");
                                        String editorString = editor.replaceAll("\\$path", path);
                                        ctx.response().headers().add("Location", editorString);
                                        ctx.response().end();
                                        return;
                                    }
                                }

                                final DocumentResource documentResource = (DocumentResource) resource;
                                if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                                    ctx.response().headers().add(ETAG_HEADER, documentResource.etag);
                                }
                                ctx.response().headers().add("Content-Length", "" + documentResource.length);
                                ctx.response().headers().add("Content-Type", mimeType);
                                final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
                                documentResource.readStream.endHandler(nothing -> {
                                    documentResource.closeHandler.handle(null);
                                    ctx.response().end();
                                });
                                pump.start();
                                // TODO: exception handlers
                            }
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("RestStorageHandler Could not find resource: " + ctx.request().uri());
                        }
                        ctx.response().setStatusCode(404);
                        ctx.response().setStatusMessage("Not Found");
                        ctx.response().end("404 Not Found");
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
        });

        router.putWithRegex(prefix + ".*").handler(ctx -> {
            ctx.request().pause();
            final String path = cleanPath(ctx.request().path().substring(prefix.length()));
            long expire = -1;
            if (ctx.request().headers().contains(EXPIRE_AFTER_HEADER)) {
                try {
                    expire = Long.parseLong(ctx.request().headers().get(EXPIRE_AFTER_HEADER));
                } catch (NumberFormatException nfe) {
                    ctx.request().resume();
                    ctx.response().setStatusCode(400);
                    ctx.response().setStatusMessage("Invalid expire after parameter: " + ctx.request().headers().get(EXPIRE_AFTER_HEADER));
                    ctx.response().end(ctx.response().getStatusMessage());
                    log.error("Expire after header, invalid value: " + ctx.response().getStatusMessage());
                    return;
                }
            }
            boolean merge = (ctx.request().query() != null && ctx.request().query().contains("merge=true")
                    && mimeTypeResolver.resolveMimeType(path).contains("application/json"));
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler put resource: " + ctx.request().uri() + " with expire: " + expire);
            }

            final String etag = ctx.request().headers().get(IF_NONE_MATCH_HEADER);

            storage.put(path, etag, merge, expire, new Handler<Resource>() {
                public void handle(Resource resource) {
                    ctx.request().resume();

                    if (!resource.modified) {
                        ctx.response().setStatusCode(304);
                        ctx.response().setStatusMessage("Not Modified");
                        ctx.response().headers().set(ETAG_HEADER, etag);
                        ctx.response().headers().add("Content-Length", "0");
                        ctx.response().end();
                        return;
                    }

                    if (!resource.exists && resource instanceof DocumentResource) {
                        ctx.response().setStatusCode(405);
                        ctx.response().setStatusMessage("Method Not Allowed");
                        ctx.response().headers().add("Allow", "GET, DELETE");
                        ctx.response().end();
                    }
                    if (resource instanceof CollectionResource) {
                        ctx.response().setStatusCode(405);
                        ctx.response().setStatusMessage("Method Not Allowed");
                        ctx.response().headers().add("Allow", "GET, DELETE");
                        ctx.response().end();
                    }
                    if (resource instanceof DocumentResource) {
                        final DocumentResource documentResource = (DocumentResource) resource;
                        documentResource.errorHandler = error -> {
                            ctx.response().setStatusCode(400);
                            ctx.response().setStatusMessage("Bad Request");
                            ctx.response().end(error);
                        };
                        documentResource.endHandler = event -> ctx.response().end();
                        final Pump pump = Pump.pump(ctx.request(), documentResource.writeStream);
                        ctx.request().endHandler(v -> documentResource.closeHandler.handle(null));
                        // TODO: exception handlers
                        pump.start();
                    }
                }
            });

        });

        router.deleteWithRegex(prefix + ".*").handler(ctx -> {
            final String path = cleanPath(ctx.request().path().substring(prefix.length()));
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler delete resource: " + ctx.request().uri());
            }
            storage.delete(path, resource -> {
                if (!resource.exists) {
                    ctx.request().response().setStatusCode(404);
                    ctx.request().response().setStatusMessage("Not Found");
                    ctx.request().response().end("404 Not Found");
                } else {
                    ctx.request().response().end();
                }
            });
        });

        router.getWithRegex(".*").handler(ctx -> {
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler resource not found: " + ctx.request().uri());
            }
            ctx.response().setStatusCode(404);
            ctx.response().setStatusMessage("Not Found");
            ctx.response().end("404 Not Found");
        });

        router.routeWithRegex(".*").handler(ctx -> respondWithNotAllowed(ctx.request()));
    }

    @Override
    public void handle(HttpServerRequest request) {
        router.accept(request);
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

    private void respondWithNotAllowed(HttpServerRequest request) {
        request.response().setStatusCode(405);
        request.response().setStatusMessage("Method Not Allowed");
        request.response().end("405 Method Not Allowed");
    }

    private void respondWithBadRequest(HttpServerRequest request, String responseMessage) {
        request.response().setStatusCode(400);
        request.response().setStatusMessage("Bad Request");
        request.response().end(responseMessage);
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
