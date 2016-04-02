package org.swisspush.reststorage;

import io.vertx.core.Handler;

import java.util.List;

public interface Storage {

    void get(String path, String etag, int offset, int count, Handler<Resource> handler);

    void storageExpand(String path, String etag, List<String> subResources, Handler<Resource> handler);

    void put(String path, String etag, boolean merge, long expire, Handler<Resource> handler);

    void delete(String path, Handler<Resource> handler);

    void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmount);

}
