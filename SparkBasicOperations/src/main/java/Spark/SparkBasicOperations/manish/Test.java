package Spark.SparkBasicOperations.manish;


public class Test {

    public static void main(String args[]){
        String response = cleanLastName("Mehra MR");
        System.out.println(response);
    }
    
    /** The Constant CHARACTERS_REMOVE_REGEX. */
    public static final String CHARACTERS_REMOVE_REGEX = "/|\\*|'|\\\"|\\\\";
    
    /** The Constant NAME_REPLACEMENT_REGEX. */
    public static final String NAME_REPLACEMENT_REGEX = "\\s*\\b(0|MRS|MR|MISS|MSTR|SR|SRA|SRITA|DR|DRA|DOC|ING|LIC|ARQ|MS|MRSDR|MRMR|MRSMRS|DRMRS|[MRS]+|[MR]+)\\b\\s*";

    /** The Constant LAST_NAME_ENDING_WITH_REGEX. */
    public static final String LAST_NAME_ENDING_WITH_REGEX = "(PHD|MSTR|SRITA|SR|MRS|MR)\\b";

    /** The Constant LAST_NAME_STARTING_WITH_REGEX. */
    public static final String LAST_NAME_STARTING_WITH_REGEX = "\\b(PHD|MSTR|SRITA|SR|MRS|MR)";
    
    /** The Constant FIRST_NAME_STARTING_WITH_REGEX. */
    public static final String FIRST_NAME_STARTING_WITH_REGEX = "\\b(PHD|MSTR|SRITA|SR|MRS|MR)";

    /** The Constant FIRST_NAME_ENDING_WITH_REGEX. */
    public static final String FIRST_NAME_ENDING_WITH_REGEX = "(PHD|MSTR|SRITA|SR|MRS|MR)\\b";
    ///** The Constant MIDDLE_NAME_REPLACEMENT_REGEX. */
    //protected static final String MIDDLE_NAME_REPLACEMENT_REGEX = "MRS\\b|\\bMRS";

    /** The Constant FULL_NAME_SPECIAL_CHARS_REGEX. */
    public static final String FULL_NAME_SPECIAL_CHARS_REGEX = "\\*|\\\"";

    //Gender Finding Regex
    /** The Constant FIRST_NAME_STARTING_GENDER_REGEX. */
    public static final String FIRST_NAME_STARTING_GENDER_REGEX = "\\b(MISS|MRS|MR)";

    /** The Constant FIRST_NAME_ENDING_GENDER_REGEX. */
    public static final String FIRST_NAME_ENDING_GENDER_REGEX = "(MISS|MRS|MR)\\b";

    /** The Constant LAST_NAME_STARTING_GENDER_REGEX. */
    public static final String LAST_NAME_STARTING_GENDER_REGEX = "\\b(MISS|MRS|MR)";

    /** The Constant LAST_NAME_ENDING_GENDER_REGEX. */
    public static final String LAST_NAME_ENDING_GENDER_REGEX = "(MISS|MRS|MR)\\b";
    
    public static String cleanMiddleName(final String name) {

        /*return name.toUpperCase().replaceAll(CHARACTERS_REMOVE_REGEX, "")
            .replaceAll(NAME_REPLACEMENT_REGEX, " ")
            .replaceAll(FULL_NAME_SPECIAL_CHARS_REGEX, " ".trim());*/
        
        return name.toUpperCase()//.replaceAll(CHARACTERS_REMOVE_REGEX, "")
            .replaceAll(NAME_REPLACEMENT_REGEX, " ")
            .replaceAll(FULL_NAME_SPECIAL_CHARS_REGEX, " ")
            .replaceAll(FIRST_NAME_STARTING_WITH_REGEX, " ")
            .replaceAll(FIRST_NAME_ENDING_WITH_REGEX, " ").trim();
    }
    
    
    private static String cleanFirstName(final String name) {

        return name.toUpperCase().replaceAll(CHARACTERS_REMOVE_REGEX, "")
            .replaceAll(NAME_REPLACEMENT_REGEX, " ")
            .replaceAll(FULL_NAME_SPECIAL_CHARS_REGEX, " ")
            .replaceAll(FIRST_NAME_STARTING_WITH_REGEX, " ")
            .replaceAll(FIRST_NAME_ENDING_WITH_REGEX, " ").trim();
    }

    /**
     * Clean last name.
     *
     * @param name - last name
     * @return the string
     */
    private static String cleanLastName(final String name) {

        return name.toUpperCase().replaceAll(CHARACTERS_REMOVE_REGEX, "")
            .replaceAll(NAME_REPLACEMENT_REGEX, " ")
            .replaceAll(FULL_NAME_SPECIAL_CHARS_REGEX, " ")
            .replaceAll(LAST_NAME_ENDING_WITH_REGEX, " ")
            .replaceAll(LAST_NAME_STARTING_WITH_REGEX, " ")
            .trim();
    }

    
    
}
