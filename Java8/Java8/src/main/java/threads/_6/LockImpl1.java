package threads._6;

public class LockImpl1 {

    public static void main(final String args[]){
        
        final MyThread1 obj = new MyThread1();
        
        new Thread(obj,"th-1").start();
        
        new Thread(obj,"th-2").start();
        
        new Thread(obj,"th-3").start();
        
        new Thread(obj,"th-4").start();
        
        new Thread(obj,"th-5").start();
        
        new Thread(obj,"th-6").start();
        
        new Thread(obj,"th-7").start();
        
        new Thread(obj,"th-8").start();
        
        new Thread(obj,"th-9").start();
        
        new Thread(obj,"th-10").start();
    }
    
}
