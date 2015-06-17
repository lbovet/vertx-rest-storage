package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.with;
import static org.vertx.testtools.VertxAssert.testComplete;

public class RedirectTest extends AbstractTestCase {

    @Test
    public void testGetHTMLResourceWithoutTrailingSlash() throws InterruptedException {
        RestAssured.basePath = "/pages";
        with().body("<h1>nemo.html</h1>").put("nemo.html");
        get("nemo.html").then().assertThat().
                statusCode(200).
                assertThat().
                contentType(ContentType.HTML);
        testComplete();
    }

    @Test
    public void testGetHTMLResourceWithTrailingSlash() throws InterruptedException {
        RestAssured.basePath = "/pages";
        with().body("<h1>nemo.html</h1>").put("nemo.html");
        get("nemo.html/").then().assertThat().
                statusCode(200).
                assertThat().
                contentType(ContentType.HTML);
        testComplete();
    }
}
