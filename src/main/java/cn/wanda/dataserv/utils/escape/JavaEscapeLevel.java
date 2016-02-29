package cn.wanda.dataserv.utils.escape;


/**
 * Created by songzhuozhuo on 2015/3/6
 */
public enum JavaEscapeLevel {


    LEVEL_1_BASIC_ESCAPE_SET(1),

    LEVEL_2_ALL_NON_ASCII_PLUS_BASIC_ESCAPE_SET(2),


    LEVEL_3_ALL_NON_ALPHANUMERIC(3),

    LEVEL_4_ALL_CHARACTERS(4);


    private final int escapeLevel;


    public static JavaEscapeLevel forLevel(final int level) {
        switch (level) {
            case 1:
                return LEVEL_1_BASIC_ESCAPE_SET;
            case 2:
                return LEVEL_2_ALL_NON_ASCII_PLUS_BASIC_ESCAPE_SET;
            case 3:
                return LEVEL_3_ALL_NON_ALPHANUMERIC;
            case 4:
                return LEVEL_4_ALL_CHARACTERS;
            default:
                throw new IllegalArgumentException("No escape level enum constant defined for level: " + level);
        }
    }


    JavaEscapeLevel(final int escapeLevel) {
        this.escapeLevel = escapeLevel;
    }

    /**
     * @return the escape level.
     */
    public int getEscapeLevel() {
        return this.escapeLevel;
    }

}

