package li.chee.vertx.reststorage;

import org.junit.Test;
import org.vertx.java.testframework.TestBase;


public class JavaRestStorageTest extends TestBase {

    @Override
    protected void setUp() throws Exception {
      super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
      super.tearDown();
    }

    @Test
    public void testSimple() throws Exception {
      start(getMethodName());
    }  
    
    private void start(String methName) throws Exception {
      startApp(TestClient.class.getName());
      startApp(TestServer.class.getName());
      startTest(methName);
    }

}
