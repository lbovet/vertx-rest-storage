package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.*;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.vertx.testtools.VertxAssert.testComplete;

public class OffsetTest extends AbstractTestCase {

    @Test
    public void testInvalidOffsets() {

        for(int i=1; i<=10; i++) {
            with().body("{ \"foo\": \"bar"+i+"\" }")
                    .put("resources/res"+i);
        }

        // get with invalid offsets
        given().param("delta", 0).when().get("resources/?limit=bla")
                .then().assertThat().body("resources", hasItem("res10"));

        given().param("delta", 0).when().get("resources/?offset=bla")
                .then().assertThat().body("resources", hasItem("res10"));

        given().param("delta", 0).when().get("resources/?offset=bla&limit=blo")
                .then().assertThat().body("resources", hasItem("res10"));

        given().param("delta", 0).when().get("resources/?offset=-99&limit-1")
                .then().assertThat().body("resources", hasItem("res10"));

        given().param("delta", 0).when().get("resources/?offset=-1&limit=-1")
                .then().assertThat().body("resources", hasItem("res10"));
        testComplete();
    }

    @Test
    public void testValidLimits() {

        for(int i=1; i<=10; i++) {
            with().body("{ \"foo\": \"bar"+i+"\" }")
                    .put("resources/res"+i);
        }

        // get with valid offsets
        given().param("delta", 0).when().get("resources/?limit=10")
                .then().assertThat()
                .body("resources", hasItems("res1","res2","res3","res4","res5","res6","res7","res8","res9","res10"));

        given().param("delta", 0).when().get("resources?limit=99")
                .then().assertThat()
                .body("resources", hasItems("res1","res2","res3","res4","res5","res6","res7","res8","res9","res10"));

        given().param("delta", 0).when().get("resources?limit=5")
                .then().assertThat()
                .body("resources", hasItems("res10","res1","res2","res3","res4"))
                .body("resources", not(hasItems("res5","res6","res7","res8","res9")));

        given().param("delta", 0).when().get("resources?limit=8")
                .then().assertThat()
                .body("resources", hasItems("res1","res10","res2","res3","res4","res5","res7"))
                .body("resources", not(hasItems("res8","res9")));
        testComplete();
    }

    @Test
    public void testValidOffsets() {

        for(int i=1; i<=10; i++) {
            with().body("{ \"foo\": \"bar"+i+"\" }")
                    .put("resources/res"+i);
        }

        // get with valid offsets
        given().param("delta", 0).when().get("resources/?offset=2")
                .then().assertThat()
                .body("resources", not(hasItems("res10","res1")))
                .body("resources", hasItems("res2","res3","res4","res5","res6","res7","res8","res9"));

        given().param("delta", 0).when().get("resources?offset=0")
                .then().assertThat()
                .body("resources", hasItems("res1","res2","res3","res4","res5","res6","res7","res8","res9","res10"));

        given().param("delta", 0).when().get("resources?offset=5")
                .then().assertThat()
                .body("resources", not(hasItems("res10","res1","res2","res3","res4")))
                .body("resources", hasItems("res5","res6","res7","res8","res9"));

        given().param("delta", 0).when().get("resources?offset=11")
                .then().assertThat()
                .body("resources", not(hasItems("res1","res2","res3","res4","res5","res6","res7","res8","res9","res10")) );
        testComplete();
    }

    @Test
    public void testInvalidLimitsOffsets() {

        for(int i=1; i<=10; i++) {
            with().body("{ \"foo\": \"bar"+i+"\" }")
                    .put("resources/res"+i);
        }

        // get with valid offsets
        given().param("delta", 0).when().get("resources/?offset=2&limit=bla")
                .then().assertThat()
                .body("resources", hasItems("res2","res3","res4","res5","res6","res7","res8","res9"));

        given().param("delta", 0).when().get("resources?offset=bla&limit=3")
                .then().assertThat()
                .body("resources", hasItems("res1","res10","res2"));

        given().param("delta", 0).when().get("resources?offset=1-5&limit=5")
                .then().assertThat()
                .body("resources", hasItems("res1","res10","res2","res3","res4"));

        given().param("delta", 0).when().get("resources?offset=99&limit=4")
                .then().assertThat()
                .body("resources", not(hasItems("res1","res2","res3","res4","res5","res6","res7","res8","res9","res10")) );
        testComplete();
    }

    @Test
    public void testValidLimitsOffsets() {

        for(int i=1; i<=10; i++) {
            with().body("{ \"foo\": \"bar"+i+"\" }")
                    .put("resources/res"+i);
        }

        // get with valid offsets
        given().param("delta", 0).when().get("resources/?offset=2&limit=-1")
                .then().assertThat()
                .body("resources", hasItems("res2","res3","res4","res5","res6","res7","res8","res9"));

        given().param("delta", 0).when().get("resources?offset=0&limit=3")
                .then().assertThat()
                .body("resources", hasItems("res1","res10","res2"));

        given().param("delta", 0).when().get("resources?offset=2&limit=2")
                .then().assertThat()
                .body("resources", hasItems("res2","res3"));

        given().param("delta", 0).when().get("resources?offset=1&limit=10")
                .then().assertThat()
                .body("resources", hasItems("res10","res2","res3","res4","res5","res6","res7","res8","res9"));
        testComplete();
    }
}
