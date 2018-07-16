package trick;

import java.util.concurrent.atomic.AtomicInteger;

public class PrintAlternate {

    public static void main(String args[]){
        AtomicInteger counter = new AtomicInteger(0);
        final String lock = "lock";

        MythreadClass obj = new MythreadClass(counter, lock);

        Thread t1 = new Thread(obj);
        t1.setName("th-1");
        
        Thread t2 = new Thread(obj);
        t2.setName("th-2");
        
        t1.start();
        t2.start();
    }
}
