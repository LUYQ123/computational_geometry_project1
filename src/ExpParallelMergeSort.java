import java.util.*;

public class ExpParallelMergeSort {
    static Long[] arr;
    static Long[][] checking;
    static int size = 1000000;
    static int pnum = 23;
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
            mergeSort(this.result, (int) (right - left));
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


    private static Long[] sequenqentMergeOnTheLastStep(MergeSortTask[] tasks){
        Long[] result=new Long[size];
        Queue<Long>[] queues=new Queue[pnum];
        for(int i = 0; i < pnum; i++)
            queues[i] = new LinkedList<>();
        for(int i=0;i<pnum;i++){
            for(int j=0;j<tasks[i].result.length;j++){
                queues[i].offer(tasks[i].result[j]);
            }
        }
        for(int i=0;i<size;i++){
            long min=Long.MAX_VALUE;
            int num=0;
            for(int j=0;j<pnum;j++){
                if(!queues[j].isEmpty() && queues[j].peek()<min){
                    min=queues[j].peek();
                    num=j;
                }
            }
            result[i]=queues[num].poll();
        }
        return result;
    }

    static public void check(Long[] truth, Long[] result){
        boolean check = true;
        if(result.length!=truth.length){
            System.out.println("length do mot match");
        }
        for (int i = 0; i < truth.length; i++) {
            if (!result[i].equals(truth[i])) {
                check = false;
                break;
            }
        }
        if (check) {
            //System.out.println("checked");
        }
        else
            System.out.println("unchecked");
    }

    static public Long[] getArray(int beginning, int length) {
        Long[] num = new Long[length];
        num[0] = (long) beginning;
        Random r = new Random();
        for (int i = 1; i < length; i++) {
            int tmp = r.nextInt(10);
            if (tmp < 7 ) num[i] = num[i - 1];
            else num[i] = num[i - 1] + r.nextInt(4);
        }
        Long[] randomPosition=new Long[length];
        Arrays.fill(randomPosition,Long.MIN_VALUE);
        for(int i=0;i<length;i++){
            int tmp=0;
            while(randomPosition[tmp]!=Long.MIN_VALUE){ tmp = r.nextInt(length);}
            randomPosition[tmp]=num[i];
        }
        return randomPosition;
    }

    public static void main(String[] args) {
        MergeSortTask[] tasks = new MergeSortTask[pnum];
        arr = getArray(0,size);
        checking = new Long[pnum][0];
        long end = System.currentTimeMillis();
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
            checking[i]=new Long[right-left];
            System.arraycopy(arr,left,checking[i],0,right-left);
            Arrays.parallelSort(checking[i]);
            left = right;
            right += size / pnum;
            tasks[i].start();
        }
        try {
            for (int i = 0; i < pnum; i++) {
                tasks[i].join();
            }
        } catch (Exception e) {
            System.out.println("Error " + e);
        }
        Long[] result= sequenqentMergeOnTheLastStep(tasks);
        long endt = System.currentTimeMillis();
        System.out.println("Took " + (endt - start) + " Millis");
        boolean rightness=true;
        for(int i=0;i<pnum;i++) {
            check(checking[i], tasks[i].result);
        }
        checking[0]=new Long[size];
        System.arraycopy(arr,0,checking[0],0,size);
        Arrays.parallelSort(checking[0]);
        check(checking[0], result);
        System.out.println();

    }
}