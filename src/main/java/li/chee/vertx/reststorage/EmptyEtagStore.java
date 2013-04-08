package li.chee.vertx.reststorage;

public class EmptyEtagStore implements EtagStore {

    @Override
    public String get(String path) {
        return "";
    }

    @Override
    public void reset(String path) {
    }

}
