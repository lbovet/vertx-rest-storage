package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.*;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public class PathLevelTest extends AbstractTestCase {

    @Before
    public void setBase() {
        RestAssured.basePath = "";
    }

    @Test
    public void testTryToPutResourceOverCollection() {

        RestAssured.basePath = "";
        with().put("/tests/crush/test1/test2/test3");
        // here we assume, that on the path server is already a collection
        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests").
                then().
                assertThat().statusCode(405);
        testComplete();
    }

    @Test
    public void testPut4levels() {
        // cleanup
        delete("");

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(200);

        // test level 1 with and without trailing slash
        assertEquals("[test2/]", get("/tests/crush/test1").body().jsonPath().get("test1").toString());
        assertEquals("[test2/]", get("/tests/crush/test1").body().jsonPath().get("test1").toString());
        assertEquals("[test2/]", get("/tests/crush/test1/").body().jsonPath().get("test1").toString());

        // test level 2 with and without trailing slash
        assertEquals("[test3/]", get("/tests/crush/test1/test2").body().jsonPath().get("test2").toString());
        assertEquals("[test3/]", get("/tests/crush/test1/test2/").body().jsonPath().get("test2").toString());

        // test level 3 with and without trailing slash
        assertEquals("[test4]", get("/tests/crush/test1/test2/test3").body().jsonPath().get("test3").toString());
        assertEquals("[test4]", get("/tests/crush/test1/test2/test3/").body().jsonPath().get("test3").toString());

        // test4 level
        assertEquals("{ \"foo\": \"bar\" }", get("/tests/crush/test1/test2/test3/test4").body().asString());

        // cleanup
        delete("");
        testComplete();
    }

    @Test
    public void testPutResourceOverCollection() {

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2").
                then().
                assertThat().statusCode(405);

        // cleanup
        delete("");
        testComplete();
    }

    @Test
    public void testPutCollectionOverResource() {

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(405);

        // cleanup
        delete("");
        testComplete();
    }
}
