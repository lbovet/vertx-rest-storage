package li.chee.vertx.reststorage;

public interface EtagStore {

    String get(String path);

    void reset(String path);

}
