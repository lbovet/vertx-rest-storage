package li.chee.vertx.reststorage;

public class UrlParser {

    public static RestStorageHandler.OffsetLimit offsetLimit(String offsetFromUrl, String limitFromUrl) {
        RestStorageHandler.OffsetLimit offsetValues = new RestStorageHandler.OffsetLimit(0, -1);
        if ((offsetFromUrl == null) && (limitFromUrl == null)) {
            return offsetValues;
        }
        int limit = -1;
        int offset = 0;
        try {
            offset = Integer.valueOf(offsetFromUrl).intValue();
            offset = offset < 0 ? 0 : offset;
        } catch (Exception e) {
            // do nothing here
        }
        
        try {
            limit = Integer.valueOf(limitFromUrl).intValue();
            limit = limit < -1 ? -1 : limit;
        } catch (Exception e) {
            // do nothing here
        }
        
        offsetValues.offset = offset;
        offsetValues.limit = limit;
        return offsetValues;
    }

    static String path(String uri) {
        int i = uri.indexOf("://");
        if (i == -1) {
            i  = 0;
        } else {
            i  = uri.indexOf('/', i + 3);
            if (i == -1) {
                // contains no /
                return "/";
            }
        }

        int queryStart = uri.indexOf('?', i);
        if (queryStart == -1) {
            queryStart = uri.length();
        }
        return uri.substring(i, queryStart);
    }

    static String query(String uri) {
        int i = uri.indexOf('?');
        if (i == -1) {
            return null;
        } else {
            return uri.substring(i + 1 , uri.length());
        }
    }

}
