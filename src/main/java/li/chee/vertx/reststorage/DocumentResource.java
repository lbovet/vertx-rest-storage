package li.chee.vertx.reststorage;

import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

public class DocumentResource extends Resource {
    public long length;
    public ReadStream readStream;
    public WriteStream writeStream;    
    public Handler<Void> closeHandler; // Called by client to close the storage
    public Handler<Void> endHandler; // Called by storage to notify
}
