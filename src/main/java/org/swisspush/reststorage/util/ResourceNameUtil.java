package org.swisspush.reststorage.util;

import java.util.List;

/**
 * <p>
 * Utility class providing handy methods to handle resource names.
 * </p>
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public final class ResourceNameUtil {

    public static final String COLON_REPLACEMENT = "§";
    public static final String SEMICOLON_REPLACEMENT = "°";

    private ResourceNameUtil() {
        // prevent instantiation
    }

    /**
     * <p>
     * Replaces all colons in the provided resourceName with {@link ResourceNameUtil#COLON_REPLACEMENT}.
     * </p>
     *
     * <pre>
     * ResourceNameUtil.replaceColonsAndSemiColons(null)          = null
     * ResourceNameUtil.replaceColonsAndSemiColons("")            = ""
     * ResourceNameUtil.replaceColonsAndSemiColons("bob")         = "bob"
     * ResourceNameUtil.replaceColonsAndSemiColons("bob_:_;_alice") = "bob_§_°_alice"
     * </pre>
     *
     * @param resourceName the String to replace the colons and semicolons, may be null
     * @return a string with the replaced values
     */
    public static String replaceColonsAndSemiColons(String resourceName){
        if(resourceName == null){
            return null;
        }
        return resourceName.replaceAll(":", COLON_REPLACEMENT).replaceAll(";", SEMICOLON_REPLACEMENT);
    }

    /**
     * <p>
     * Replaces all colons and semicolons in all strings of the provided resourceNames with {@link ResourceNameUtil#COLON_REPLACEMENT} and {@link ResourceNameUtil#SEMICOLON_REPLACEMENT}.
     * </p>
     *
     * @param resourceNames the list of strings to replace the colons and semicolons, may be null
     */
    public static void replaceColonsAndSemiColonsInList(List<String> resourceNames){
        if(resourceNames != null){
            resourceNames.replaceAll(ResourceNameUtil::replaceColonsAndSemiColons);
        }
    }

    /**
     * <p>
     * Resets the replaced colons and semicolons in the provided resourceName with a colon or semicolon.
     * </p>
     *
     * <pre>
     * ResourceNameUtil.resetReplacedColonsAndSemiColons(null)          = null
     * ResourceNameUtil.resetReplacedColonsAndSemiColons("")            = ""
     * ResourceNameUtil.resetReplacedColonsAndSemiColons("bob")         = "bob"
     * ResourceNameUtil.resetReplacedColonsAndSemiColons("bob_§_°_alice") = "bob_:_;_alice"
     * </pre>
     *
     * @param resourceName the String to reset the replaced the colons and semicolons, may be null
     * @return a string with the resetted values
     */
    public static String resetReplacedColonsAndSemiColons(String resourceName){
        if(resourceName == null){
            return null;
        }
        return resourceName.replaceAll(COLON_REPLACEMENT, ":").replaceAll(SEMICOLON_REPLACEMENT, ";");
    }

    /**
     * <p>
     * Resets the replaced colons and semicolons in all strings of the provided resourceNames with a colon or semicolon.
     * </p>
     *
     * @param resourceNames the list of strings to reset the colons and semicolons, may be null
     */
    public static void resetReplacedColonsAndSemiColonsInList(List<String> resourceNames){
        if(resourceNames != null){
            resourceNames.replaceAll(ResourceNameUtil::resetReplacedColonsAndSemiColons);
        }
    }
}
