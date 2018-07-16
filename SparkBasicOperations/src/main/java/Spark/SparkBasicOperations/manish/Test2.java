package Spark.SparkBasicOperations.manish;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class Test2 {

    public static void main(final String args[]){
         
        final Set<String> initKeyWordsList = new HashSet<>();
        initKeyWordsList.add("alonso");
        
        final boolean response  = isValidEmailAddressWithoutInvalidKeywords("mkyong@com.m1",initKeyWordsList);
        System.out.println(response);
    }
    
    /** The Constant emailPattern. */
    public static final Pattern emailPattern = Pattern.compile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
        + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z0-9]{2,})$");
    
    /**
     * Checks if is valid email address.
     *
     * @param emailAddress the email address
     * @return true, if is valid email address
     */
    public static boolean isValidEmailAddress(final String emailAddress) {// throws Exception {
        return emailPattern.matcher(emailAddress).matches();
    }
    
    /**
     * Checks if is valid email address and not contain any invalid keywords.
     *
     * @param emailAddress the email address
     * @param initKeyWordsList 
     * @return true, if is valid email address
     */
    public static boolean isValidEmailAddressWithoutInvalidKeywords(final String emailAddress, final Set<String> initKeyWordsList) {// throws Exception {
        return emailPattern.matcher(emailAddress).matches()
                && !(initKeyWordsList!=null && initKeyWordsList.parallelStream().anyMatch(keyword -> emailAddress.toLowerCase().contains(keyword)));
    }
}
