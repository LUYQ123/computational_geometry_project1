import java.util.Arrays;
import java.util.Random;

public class ExpParallelMergeSort {
    static long[] arr;
    static int size = 1 << 6;
    static int pnum = 10;
    private static class MergeSortTask extends Thread {
        private long left;
        private long right;
        private  Long[] result;

        public MergeSortTask(long left, long right) {
            this.left = left;
            this.right = right;
            this.result = new Long[(int) (right-left)];
            for(int i= 0;i<(right-left);i++){
                this.result[i]=arr[(int) (i+left)];
            }
        }

        @Override
        public void run() {
            mergeSort(this.result, (int) (right-left));
        }

        public Long[] getResult() {
            return result;
        }
    }

    public static void mergeSort(Long[] a, int n) {
        if(n<2)
            return;
        int mid = n / 2;
        Long[] l = new Long[mid];
        Long[] r = new Long[n - mid];

        for (int i = 0; i < mid; i++) {
            l[i] = a[i];
        }
        for (int i = mid; i < n; i++) {
            r[i - mid] = a[i];
        }
        mergeSort(l, mid);
        mergeSort(r, n - mid);

        merge(a, l, r, mid, n - mid);
    }


    public static void  merge(Long[] a, Long[] l, Long[] r, int left, int right) {
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


    private static Long[] sequenqentMergeOnTheLastStep(MergeSortTask tasks[]){
        Long[] result=new Long[tasks[0].result.length];
        System.arraycopy(tasks[0].result,0,result,0,tasks[0].result.length);
        for(int i=1;i<pnum;i++){
            Long[] tmp=new Long[result.length+tasks[i].result.length];
           // System.out.println(i);
            merge(tmp,result,tasks[i].result,result.length,tasks[i].result.length);
            result=new Long[tmp.length];
            System.arraycopy(tmp,0,result,0,tmp.length);
        }
        return result;
    }

    public static void main(String[] args) {


        Boolean[] exist=new Boolean[size];
        Arrays.fill(exist,false);
        MergeSortTask tasks[] = new MergeSortTask[pnum];

        arr = new long[size];
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            while(true) {
                int tmp = r.nextInt(size);
                if (!exist[tmp]) {
                    arr[i] = tmp;
                    exist[tmp]=true;
                    break;
                }

            }
        }
        long start = System.currentTimeMillis();

        int left=0;
        int right=size/pnum;
        int last=size%pnum;
        for (int i = 0; i < pnum; i++) {
            if (last > 0) {
                right++;
                last--;
            }
            tasks[i] = new MergeSortTask(left, right);
          //  System.out.println(left+"  "+right);
            left = right;
            right += size / pnum;
            tasks[i].start();
        }
//        for (int i = 0; i < pnum; i++) {
//            tasks[i] = new MergeSortTask(size / pnum * i, size / pnum * (i + 1));
//            tasks[i].start();
//        }

        try {
            for (int i = 0; i < pnum; i++) {
                tasks[i].join();
            }
        } catch (Exception e) {
            System.out.println("Error " + e);
        }

//        for(int i=0;i<pnum;i++){
//            System.out.print("thread "+i+": ");
//            for(int j=0;j<tasks[i].result.length;j++){
//                System.out.print(tasks[i].result[j]+" ");
//            }
//            System.out.println();
//        }
//

        Long[] result= sequenqentMergeOnTheLastStep(tasks);
        long endt = System.currentTimeMillis();
        System.out.print("Sorted array: " );
        boolean rightness=true;
        for(int i=1;i<result.length;i++){
            if(result[i]-result[i-1]!=1){
                rightness=false;
            }
        }
        if(rightness && result[result.length-1]==size-1){
            System.out.println("Sorted");
        }
        else{
            System.out.println("Wrong");
        }
        System.out.println();
        System.out.println("Took " + (endt - start) + " Millis");
    }
}