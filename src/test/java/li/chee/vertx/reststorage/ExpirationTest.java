package li.chee.vertx.reststorage;

import static com.jayway.awaitility.Duration.TWO_SECONDS;
import com.jayway.restassured.RestAssured;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.restassured.RestAssured.*;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.awaitility.Awaitility.*;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.vertx.testtools.VertxAssert.testComplete;

public class ExpirationTest extends AbstractTestCase {

    @Test
    public void testPutInvalidExpireFloat() {

        given().
                header("x-expire-after", "1.22").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireisfloat").
                then().
                assertThat().statusCode(400);

        when().get("expireaftertwoseconds").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testPutInvalidExpireNaN() {

        given().
                header("x-expire-after", "asdfasdf").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("invalidExpireNan").
                then().
                assertThat().statusCode(400);

        when().get("invalidExpireNan").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testPutExpireAfterOneSecond() {

        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireaftertwoseconds").
                then().
                assertThat().statusCode(200);

        await().atMost(TWO_SECONDS).until(new Callable<Integer>() {
            public Integer call() throws Exception {
                return get("expireaftertwoseconds").statusCode();
            }
        }, equalTo(404));
        testComplete();
    }

    @Test
    public void testPutAfterExpiration() {

        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("putafterexpiration").
                then().
                assertThat().statusCode(200);


        await().atMost(3, TimeUnit.SECONDS).until(new Callable<Integer>() {
            public Integer call() throws Exception {
                return get("putafterexpiration").statusCode();
            }
        }, equalTo(404));

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("putafterexpiration").
                then().
                assertThat().statusCode(200);

        when().get("putafterexpiration").then().statusCode(200).body("foo", equalTo("bar2"));
        testComplete();
    }

    @Test
    public void testPutAfterImmediateExpiration() {

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireimmediatly").
                then().
                assertThat().statusCode(200);


        await().atMost(TWO_SECONDS).until(new Callable<Integer>() {
            public Integer call() throws Exception {
                return get("expireimmediatly").statusCode();
            }
        }, equalTo(404));

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("expireimmediatly").
                then().
                assertThat().statusCode(200);

        when().get("/expireimmediatly").then().statusCode(200).body("foo", equalTo("bar2"));
        testComplete();
    }

    @Test
    public void testPutMultipleBranchesAfterImmediateExpiration() {

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("resexpireimmediatly/branch2").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar3\" }").
                when().
                put("resexpireimmediatly/branch3").
                then().
                assertThat().statusCode(200);


        await().atMost(TWO_SECONDS).until(new Callable<Integer>() {
            public Integer call() throws Exception {
                return get("resexpireimmediatly/branch1").statusCode();
            }
        }, equalTo(404));

        when().get("resexpireimmediatly/branch2").then().assertThat().statusCode(404);
        when().get("resexpireimmediatly/branch3").then().assertThat().statusCode(404);

        given().
                header("x-expire-after", "100").
                body("{ \"foo\": \"bar11\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar22\" }").
                when().
                put("resexpireimmediatly/branch2").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar33\" }").
                when().
                put("resexpireimmediatly/branch3").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().statusCode(200).body("foo", equalTo("bar11"));
        when().get("resexpireimmediatly/branch2").then().statusCode(200).body("foo", equalTo("bar22"));
        when().get("resexpireimmediatly/branch3").then().statusCode(200).body("foo", equalTo("bar33"));
        testComplete();
    }

    @Test
    public void testPutWithNoExpiryAfterImmediateExpiration() {

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().assertThat().statusCode(404);

        given().
                body("{ \"foo\": \"bar11\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().statusCode(200).body("foo", equalTo("bar11"));
        testComplete();
    }
}
