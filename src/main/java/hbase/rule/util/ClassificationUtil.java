package hbase.rule.util;
public class ClassificationUtil {
    
    
    public ClassificationUtil() {
        // empty
    }

    public static boolean isSimilar(String x, String y) {
        return x.contains(y);
    }
    
    public static int isSimilar(float x, float y) {
        return Float.compare(x, y);
    }
    
    public static int isSimilar(int x, int y) {
        return Integer.compare(x, y);
    }
}