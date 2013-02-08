package li.chee.vertx.reststorage;

import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

public class DocumentResource extends Resource {
    public int length;
    public ReadStream readStream;
    public WriteStream writeStream;    
}
