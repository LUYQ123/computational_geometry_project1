import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ForkJoinPool;

public class ParallelMin {
    static Long[] arr;

    private static class MinTask extends RecursiveTask<Long> {
        private long left;
        private long right;

        public MinTask(long left, long right){
            this.left = left;
            this.right = right;
        }

        @Override
        protected Long compute(){
            if (left < right) {
                long mid = (left + right)/2;
                MinTask leftTask = new MinTask(left,mid);
                MinTask rightTask = new MinTask(mid+1,right);
                invokeAll(leftTask,rightTask);
                Long leftVal = leftTask.join();
                Long rightVal = rightTask.join();
                Long val = leftVal.longValue()<rightVal.longValue()? Long.valueOf(leftVal.longValue()) : Long.valueOf(rightVal.longValue());
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
        arr[0]=Long.valueOf(0);
        arr[2]=Long.valueOf(2);

        long start = System.currentTimeMillis();

        ForkJoinPool pool = new ForkJoinPool();
        MinTask topTask = new MinTask(0,arr.length-1);
        pool.invoke(topTask);

        Long sum = topTask.join();

        long endt = System.currentTimeMillis();

        System.out.println("Min: "+sum);
        // System.out.println(size);
        System.out.println("Took "+(endt-start)+ " Millis");
    }
}


//    int left=0;
//    int right=size/pnum;
//    int last=size%pnum;
//        for (int i = 0; i < pnum; i++) {
//        if(last>0){
//        right++;
//        last--;
//        }
//        tasks[i] = new MergeSortTask(left, right);
//        left=right+1;
//        right+=size/pnum;
//        tasks[i].start();
//        }