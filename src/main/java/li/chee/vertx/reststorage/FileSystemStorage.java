package li.chee.vertx.reststorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.file.FileProps;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.deploy.impl.VertxLocator;

public class FileSystemStorage implements Storage {

    private String root;

    public FileSystemStorage(String root) {
        this.root = root;
    }

    @Override
    public void get(String path, final Handler<AsyncResult<Resource>> handler) {
        final String fullPath = canonicalize(path);
        fileSystem().exists(fullPath, new AsyncResultHandler<Boolean>() {
            public void handle(AsyncResult<Boolean> event) {
                if (event.result) {
                    fileSystem().props(fullPath, new AsyncResultHandler<FileProps>() {
                        public void handle(AsyncResult<FileProps> event) {
                            final FileProps props = event.result;
                            if (props.isDirectory) {
                                fileSystem().readDir(fullPath, new AsyncResultHandler<String[]>() {
                                    public void handle(AsyncResult<String[]> event) {
                                        CollectionResource c = new CollectionResource();
                                        c.items = new ArrayList<String>(event.result.length);
                                        int dirLength = fullPath.length();
                                        for (String item : event.result) {
                                            c.items.add(item.substring(dirLength + 1));
                                        }
                                        handler.handle(new AsyncResult<Resource>(c));
                                    }
                                });
                            } else if (props.isRegularFile) {
                                fileSystem().open(fullPath, new AsyncResultHandler<AsyncFile>() {
                                    public void handle(final AsyncResult<AsyncFile> event) {
                                        System.out.println(props.size);
                                        DocumentResource d = new DocumentResource();
                                        d.length = props.size;
                                        d.readStream = event.result.getReadStream();
                                        d.closeHandler = new SimpleHandler() {
                                            protected void handle() {
                                                event.result.close();
                                            }
                                        };
                                        handler.handle(new AsyncResult<Resource>(d));
                                    }
                                });
                            } else {
                                Resource r = new Resource();
                                r.exists = false;
                                handler.handle(new AsyncResult<Resource>(r));
                            }
                        }
                    });
                } else {
                    Resource r = new Resource();
                    r.exists = false;
                    handler.handle(new AsyncResult<Resource>(r));
                }
            }
        });
    }

    @Override
    public void put(String path, final Handler<AsyncResult<Resource>> handler) {
        final String fullPath = canonicalize(path);
        fileSystem().exists(fullPath, new AsyncResultHandler<Boolean>() {
            public void handle(AsyncResult<Boolean> event) {
                if (event.result) {
                    fileSystem().props(fullPath, new AsyncResultHandler<FileProps>() {
                        public void handle(AsyncResult<FileProps> event) {
                            final FileProps props = event.result;
                            if (props.isDirectory) {
                                CollectionResource c = new CollectionResource();
                                handler.handle(new AsyncResult<Resource>(c));
                            } else if (props.isRegularFile) {
                                putFile(handler, fullPath);
                            } else {
                                Resource r = new Resource();
                                r.exists = false;
                                handler.handle(new AsyncResult<Resource>(r));
                            }
                        }
                    });
                } else {
                    final String dirName = dirName(fullPath);
                    fileSystem().exists(dirName, new AsyncResultHandler<Boolean>() {
                        public void handle(AsyncResult<Boolean> event) {
                            if (event.result) {
                                putFile(handler, fullPath);
                            } else {
                                fileSystem().mkdir(dirName, true, new AsyncResultHandler<Void>() {
                                    public void handle(AsyncResult<Void> event) {
                                        putFile(handler, fullPath);
                                    }
                                });
                            }
                        }
                    });
                }
            }
        });
    }

    private void putFile(final Handler<AsyncResult<Resource>> handler, final String fullPath) {
        final String tempFile = fullPath + "." + UUID.randomUUID().toString();
        fileSystem().open(tempFile, new AsyncResultHandler<AsyncFile>() {
            public void handle(final AsyncResult<AsyncFile> event) {
                if (event.succeeded()) {
                    System.out.println(fullPath);
                    DocumentResource d = new DocumentResource();
                    d.writeStream = event.result.getWriteStream();
                    d.closeHandler = new SimpleHandler() {
                        protected void handle() {
                            event.result.close(new AsyncResultHandler<Void>() {
                                public void handle(AsyncResult<Void> event) {
                                    fileSystem().delete(fullPath, new AsyncResultHandler<Void>() {
                                        public void handle(AsyncResult<Void> event) {
                                            fileSystem().move(tempFile, fullPath, new AsyncResultHandler<Void>() {
                                                public void handle(AsyncResult<Void> event) {
                                                    // do nothing
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    };
                    handler.handle(new AsyncResult<Resource>(d));
                } else {
                    Resource r = new Resource();
                    r.exists = false;
                    handler.handle(new AsyncResult<Resource>(r));
                }
            }
        });
    }

    @Override
    public void delete(String path, final Handler<AsyncResult<Resource>> handler) {
        final String fullPath = canonicalize(path);
        fileSystem().exists(fullPath, new AsyncResultHandler<Boolean>() {
            public void handle(AsyncResult<Boolean> event) {
                if (event.result) {
                    fileSystem().delete(fullPath, true, new AsyncResultHandler<Void>() {
                        public void handle(AsyncResult<Void> event) {
                            Resource resource = new Resource();
                            if(event.failed()) {
                                resource.exists = false;
                            }
                            handler.handle(new AsyncResult<Resource>(resource));
                        }
                    });
                }
            }
        });
    }

    private String canonicalize(String path) {
        try {
            return new File(root + path).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String dirName(String path) {
        return new File(path).getParent();
    }

    private FileSystem fileSystem() {
        return VertxLocator.vertx.fileSystem();
    }
}
