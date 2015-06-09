package li.chee.vertx.reststorage;

import com.jayway.restassured.RestAssured;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.vertx.testtools.VertxAssert.testComplete;

public class CrudTest extends AbstractTestCase {

    @Before
    public void setPath() {
        RestAssured.basePath = "";
    }

    @After
    public void deleteCrudPath() {
        RestAssured.basePath = "";
        delete("/");
    }


    @Test
    public void testPutGetDelete() {
        with().body("{ \"foo\": \"bar\" }").put("res");
        when().get("res").then().assertThat().body("foo", equalTo("bar"));
        delete("res");
        when().get("res").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testList() {
        with().body("{ \"foo\": \"bar\" }").put("resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("resources/res2");
        when().get("resources").then().assertThat().body("resources", hasItems("res1", "res2"));
        testComplete();
    }

    @Test
    public void testMerge() {
        with().body("{ \"foo\": \"bar\", \"hello\": \"world\" }").put("res");
        with().
                param("merge", "true").
                body("{ \"foo\": \"bar2\" }").
                put("res");
        get("res").then().assertThat().
                statusCode(200).
                body("foo", equalTo("bar2")).
                body("hello", equalTo("world"));
        testComplete();
    }

    @Test
    public void testTwoBranchesDeleteOnLeafOfOneBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/leaf");

        delete("branch2");

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/leaf").then().assertThat().statusCode(404);

        delete("branch2/res/leaf");
        testComplete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeUnderLeafOfOneBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");

        delete("branch2/res");

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/res2").then().assertThat().statusCode(404);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeAfterBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");

        delete("branch1/res");

        RestAssured.basePath = "";

        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("branch1").then().assertThat().statusCode(404);
        when().get("branch1/res").then().assertThat().statusCode(404);
        when().get("branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testTwoBranchesDeleteOnRoot() {

        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");

        RestAssured.basePath = "";

        delete("/");

        when().get("node").then().assertThat().statusCode(404);
        when().get("node/node").then().assertThat().statusCode(404);
        when().get("node/node/node").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        testComplete();
    }


    @Test
    public void testThreeBranchesDeleteOnLeafOfOneBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("node/branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("node/branch2/res/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("node/branch3/res/leaf");

        delete("node/branch3/res/leaf");

        RestAssured.basePath = "";

        when().get("node/branch1").then().assertThat().statusCode(200);
        when().get("node/branch1/res").then().assertThat().statusCode(200);
        when().get("node/branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("node/branch2").then().assertThat().statusCode(200);
        when().get("node/branch2/res").then().assertThat().statusCode(200);
        when().get("node/branch2/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("node/branch3").then().assertThat().statusCode(404);
        when().get("node/branch3/res").then().assertThat().statusCode(404);
        when().get("node/branch3/res/leaf").then().assertThat().statusCode(404);
        testComplete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeUnderLeafOfOneBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");

        delete("branch1/res");

        RestAssured.basePath = "";

        when().get("/branch1").then().assertThat().statusCode(404);
        when().get("/branch1/res").then().assertThat().statusCode(404);
        when().get("/branch1/res/res1/").then().assertThat().statusCode(404);
        when().get("/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        when().get("/branch2").then().assertThat().statusCode(200);
        when().get("/branch2/res").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("/branch3").then().assertThat().statusCode(200);
        when().get("/branch3/res").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        testComplete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeAfterBranch() {

        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");

        delete("branch2/res");

        RestAssured.basePath = "";


        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/res2").then().assertThat().statusCode(404);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("branch3").then().assertThat().statusCode(200);
        when().get("branch3/res").then().assertThat().statusCode(200);
        when().get("branch3/res/res3").then().assertThat().statusCode(200);
        when().get("branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        testComplete();
    }

    @Test
    public void testThreeBranchesDeleteOnRoot() {

        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch3/res/res3/leaf");

        RestAssured.basePath = "";

        delete("/");

        when().get("node").then().assertThat().statusCode(404);
        when().get("node/node").then().assertThat().statusCode(404);
        when().get("node/node/node").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res/res3/leaf").then().assertThat().statusCode(404);
        testComplete();
    }
}
