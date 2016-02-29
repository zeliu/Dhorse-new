package cn.wanda.dataserv.core;

public class AssertUtils {
    public static void assertTrue(boolean val, String msg) {
        if (!val) {
            throw new AssertException(msg);
        }
    }

    public static void assertNotNull(Object o, String msg) {
        if (o == null) {
            throw new AssertException(msg);
        }
    }

    public static void notReached() {
        throw new AssertException("this code should not be reached");
    }

}
