package li.chee.vertx.reststorage;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

public class DocumentResource extends Resource {
    public long length;
    public String etag;
    public ReadStream readStream;
    public WriteStream writeStream;    
    public Handler<Void> closeHandler; // Called by client to close the storage
    public Handler<Void> endHandler; // Called by storage to notify
}
