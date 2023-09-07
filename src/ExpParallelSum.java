import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ForkJoinPool;

public class ExpParallelSum {
    static long[] arr;
    
    private static class SumTask extends Thread {
        private long left;
        private long right;
        private long result;

        public SumTask(long left, long right){
            this.left = left;
            this.right = right;
            this.result = 0;
        }

        @Override
        public void run(){
            for (long i=left ; i< right ; i++) {
                result +=arr[(int)i];
            }
        }

        public long getResult(){
            return result;
        }
    }


    public static void main(String[] args){
        int size = 1<<27;
        int pnum = 16;

        SumTask tasks[] = new SumTask[pnum];

        arr = new long[size];
        for (int i=0 ; i<arr.length ; i++){
            arr[i]=1;
        }

        long start = System.currentTimeMillis();

        for (int i=0 ; i<pnum ; i++) {
            tasks[i] = new SumTask(size/pnum*i,size/pnum*(i+1));
            tasks[i].start();
        }

        try {
            for (int i=0; i<pnum ; i++) {
                tasks[i].join();
            }
        } catch (Exception e){
            System.out.println("Error " + e);
        }

        long endt = System.currentTimeMillis();

        long finalres = 0;
        for (int i=0 ; i<pnum ; i++){
            finalres += tasks[i].getResult();
        }

        System.out.println("Sum: "+finalres);
        System.out.println("Took "+(endt-start)+ " Millis");

        long sum=0;
        start = System.currentTimeMillis();

        for(int i=0;i<size;i++){
            sum+=1;
        }
        endt=System.currentTimeMillis();
        System.out.println("Sum: "+sum);
        System.out.println(size);
        System.out.println("Took "+(endt-start)+ " Millis");
    }
}




