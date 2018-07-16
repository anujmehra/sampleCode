package threads._6;

public class LockImpl2Fairness {

    public static void main(String args[]){
        
        final MyThread2 obj = new MyThread2();
        
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
