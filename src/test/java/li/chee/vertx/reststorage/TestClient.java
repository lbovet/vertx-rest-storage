package li.chee.vertx.reststorage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.framework.TestClientBase;

public class TestClient extends TestClientBase {

    private EventBus eb;

    @Override
    public void start() {
        super.start();
        eb = vertx.eventBus();

    }

    @Override
    public void stop() {
        super.stop();
    }

   
}
