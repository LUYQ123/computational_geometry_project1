import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class ParallelMergeSort {
       static Long[] array;
    private static class MergeSortTask extends RecursiveTask<Long[]> {
        private long left;
        private long right;

        private  Long[] arr;

        public MergeSortTask(long left, long right, Long[] arr){
            this.left = left;
            this.right = right;
            this.arr=new Long[(int) (right-left)+1];
           // System.out.println("left: "+left+" right: "+right);
            System.arraycopy(arr, (int) left,this.arr,0, (int) (right-left));
//            for(int i = (int) left; i<right; i++){
//                long tmp=arr[i];
//                this.arr[(int) (i-left)]=tmp;
//            }

        }

        @Override
        protected Long[] compute(){
            if (left < right) {
                long mid = (left + right)/2;
                MergeSortTask leftTask = new MergeSortTask(left,mid,array);
                MergeSortTask rightTask = new MergeSortTask(mid+1,right,array);
                invokeAll(leftTask,rightTask);
                Long[] leftVal = leftTask.join();
                Long[] rightVal = rightTask.join();
                merge(this.arr,leftVal,rightVal,leftVal.length,rightVal.length);
//                System.out.println("left: "+left+" right: "+right);
//                if(right-left>0) {
//                    String str="left: " + left + " right: " + right+" arrays: ";
//                    for (int i = 0; i < this.arr.length; i++) {
//                        str+=this.arr[i] + " ";
//                    }
//                    System.out.println(str);
//                }
                return this.arr;

            } else {
//                System.out.println("left: "+left+" right: "+right);
                return new Long[]{array[(int) left]};
            }

        }
    }

    public static void merge(Long[] a, Long[] l, Long[] r, int left, int right) {

        int i = 0, j = 0, k = 0;
        while (i < left && j < right) {
            if (l[i] <= r[j]) {
                a[k++] = l[i++];
            }
            else {
                a[k++] = r[j++];
            }
        }
        while (i < left) {
            a[k++] = l[i++];
        }
        while (j < right) {
            a[k++] = r[j++];
        }
    }

    public static void main(String[] args){
        int size = 1 << 25;

        Boolean[] exist=new Boolean[size];
        Arrays.fill(exist,false);

        array = new Long[size];
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            while(true) {
                int tmp = r.nextInt(size);
                if (!exist[tmp]) {
                    array[i] = Long.valueOf(tmp);
                    exist[tmp]=true;
                    break;
                }

            }
        }

        long start = System.currentTimeMillis();

        ForkJoinPool pool = new ForkJoinPool();
        MergeSortTask topTask = new MergeSortTask(0,size-1,array);
        pool.invoke(topTask);

        Long[] result = topTask.join();

        long endt = System.currentTimeMillis();

        System.out.print("Sorted array: " );
        boolean rightness=true;
        for(int i=1;i<result.length;i++){
            if(result[i]-result[i-1]!=1){
                rightness=false;
            }
        }
        if(rightness){
            System.out.println("Sorted");
        }
        else{
            System.out.println("Wrong");
        }
        System.out.println();

        // System.out.println(size);
        System.out.println("Took "+(endt-start)+ " Millis");
    }

}