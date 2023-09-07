import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ForkJoinPool;

public class ParallelSum {
    static Long[] arr;
    
    private static class SumTask extends RecursiveTask<Long> {
        private long left;
        private long right;

        public SumTask(long left, long right){
            this.left = left;
            this.right = right;
        }

        @Override
        protected Long compute(){
            if (left < right) {
                long mid = (left + right)/2;
                SumTask leftTask = new SumTask(left,mid);
                SumTask rightTask = new SumTask(mid+1,right);
                invokeAll(leftTask,rightTask);
                Long leftVal = leftTask.join();
                Long rightVal = rightTask.join();
                Long val = Long.valueOf(leftVal.longValue()+rightVal.longValue());
                return val;
            } else {
                return arr[(int)left];
            }
        }
    }


    public static void main(String[] args){
        int size = 1<<27;
        arr = new Long[size];
        for (int i=0 ; i<arr.length ; i++){
            arr[i]= Long.valueOf(1);
            //System.out.println(arr[i]);
        }

        long start = System.currentTimeMillis();

        ForkJoinPool pool = new ForkJoinPool();
        SumTask topTask = new SumTask(0,arr.length-1);
        pool.invoke(topTask);

        Long sum = topTask.join();

        long endt = System.currentTimeMillis();

        System.out.println("Sum: "+sum);
        System.out.println(size);
        System.out.println("Took "+(endt-start)+ " Millis");



    }
}




