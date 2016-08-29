package org.swisspush.reststorage;

import com.jayway.awaitility.Duration;
import com.jayway.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.restassured.RestAssured.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;

@RunWith(VertxUnitRunner.class)
public class CrudTest extends AbstractTestCase {

    @Test
    public void testDoubleSlashesHandling(TestContext context) {
        Async async = context.async();

        // put with double slashes
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar\" }").put("resources//myres");

        // get with single/double slashes should both work
        when().get("resources/myres").then().assertThat().body("foo", equalTo("bar"));
        given().urlEncodingEnabled(false).when().get("resources//myres").then().assertThat().body("foo", equalTo("bar"));

        // delete with double slashes. after this, the resource should be gone
        given().urlEncodingEnabled(false).delete("resources//myres").then().assertThat().statusCode(200);
        given().delete("resources/myres").then().assertThat().statusCode(404);

        // get again with double slashes
        when().get("resources/myres").then().assertThat().statusCode(404);
        given().urlEncodingEnabled(false).when().get("resources//myres").then().assertThat().statusCode(404);

        async.complete();
    }

    @Test
    public void testPutGetDelete(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("res");
        when().get("res").then().assertThat().body("foo", equalTo("bar"));
        delete("res");
        when().get("res").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testList(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("resources/res2");

        checkGETStatusCodeWithAwait5sec("resources/res1", 200);
        checkGETStatusCodeWithAwait5sec("resources/res2", 200);

        when().get("resources").then().assertThat().body("resources", hasItems("res1", "res2"));
        async.complete();
    }

    @Test
    public void testMerge(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\", \"hello\": \"world\" }").put("res");
        checkGETStatusCodeWithAwait5sec("res", 200);
        with().
                param("merge", "true").
                body("{ \"foo\": \"bar2\" }").
                put("res");
        checkGETStatusCodeWithAwait5sec("res", 200);
        get("res").then().assertThat().
                statusCode(200).
                body("foo", equalTo("bar2")).
                body("hello", equalTo("world"));
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/leaf", 200);

        delete("branch2");
        checkGETStatusCodeWithAwait5sec("branch2", 404);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/leaf").then().assertThat().statusCode(404);

        delete("branch2/res/leaf");
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeUnderLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        delete("branch2/res");
        checkGETStatusCodeWithAwait5sec("branch2/res", 404);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/res2").then().assertThat().statusCode(404);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeAfterBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        delete("branch1/res");
        checkGETStatusCodeWithAwait5sec("branch1/res", 404);

        RestAssured.basePath = "";

        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("branch1").then().assertThat().statusCode(404);
        when().get("branch1/res").then().assertThat().statusCode(404);
        when().get("branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnRoot(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);

        RestAssured.basePath = "";

        delete("/");
        checkGETStatusCodeWithAwait5sec("/", 404);

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
        async.complete();
    }


    @Test
    public void testThreeBranchesDeleteOnLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("node/branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("node/branch2/res/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("node/branch3/res/leaf");
        checkGETStatusCodeWithAwait5sec("node/branch1/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/branch2/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/branch3/res/leaf", 200);

        delete("node/branch3/res/leaf");
        checkGETStatusCodeWithAwait5sec("node/branch3/res/leaf", 404);

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
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeUnderLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        delete("branch1/res");
        checkGETStatusCodeWithAwait5sec("branch1/res", 404);

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
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeAfterBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        delete("branch2/res");
        checkGETStatusCodeWithAwait5sec("branch2/res", 404);

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
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnRoot(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch3/res/res3/leaf", 200);

        RestAssured.basePath = "";

        delete("/");
        checkGETStatusCodeWithAwait5sec("/", 404);

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
        async.complete();
    }

    private void checkGETStatusCodeWithAwait5sec(final String request, final Integer statusCode) {
        await().atMost(Duration.FIVE_SECONDS).until(() -> String.valueOf(when().get(request).getStatusCode()), equalTo(String.valueOf(statusCode)));
    }
}
