import java.util.*;

public class TotalParallelMergeSort {
    static Long[] arr;
    static Long[] checking;
    static int pnum = 10;

    static Long[] result;

    static int size=200000;

    static int beginning=-9999;

    static Queue<Long[]> que= new LinkedList<>();
    static Long[] a;
    static Long[] b;

    private static class MergeTask extends Thread {
        int[] Class;
        int[] rank;

        int time=0;
        public MergeTask(int[] Class, int[] rank) {
            this.Class=Class;
            this.rank=rank;
        }

        public void run() {
            int lastFind = 0;
            for (int i = 0; i < this.Class.length; i++) {
                if (i > 0 && this.Class[i] == this.Class[i - 1] &&
                        ((this.Class[i] == 0 && a[rank[i]].equals(a[rank[i] - 1]) ) ||
                                (this.Class[i] == 1 && b[rank[i]].equals(b[rank[i] - 1])))) {
                    boolean tmp = true;
                    long start=System.currentTimeMillis();
                    while (lastFind < result.length && tmp) {
                        synchronized (result[lastFind]) {
                            if (result[lastFind] != Long.MIN_VALUE) {lastFind++;}
                              else {
                                result[lastFind] = this.Class[i] == 0 ? a[rank[i]] : b[rank[i]];
                                tmp = false;
                            }
                        }
                    }
                    long end=System.currentTimeMillis();
                    time+=end-start;
                }
                else if (this.Class[i] == 0) {
                    int left = 0;
                    int right = b.length - 1;
                    while (right > left) {
                        int mid = (right + left) / 2;
                        if (a[rank[i]] <= b[mid]) {
                            right = mid - 1;
                        } else if (a[rank[i]] > b[mid]) {
                            left = mid + 1;
                        }
                    }
                    while (left < b.length && a[rank[i]] > b[left]) left++;
                    boolean tmp = true;
                    long start=System.currentTimeMillis();
                    while (tmp) {

                        synchronized (result[rank[i] + left]) {

                            if (result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                result[rank[i] + left] = a[rank[i]];
                                tmp = false;
                            }
                        }
                    }
                    long end=System.currentTimeMillis();
                    time+=end-start;
                    lastFind = rank[i] + left;
                }
                else if (this.Class[i] == 1) {
                    int left = 0;
                    int right = a.length - 1;
                    while (right > left) {
                        int mid = (right + left) / 2;
                        if (b[rank[i]] <= a[mid]) {
                            right = mid - 1;
                        } else if (b[rank[i]] > a[mid]) {
                            left = mid + 1;
                        }
                    }
                    while (left < a.length && b[rank[i]] > a[left]) left++;
                    boolean tmp = true;
                    long start=System.currentTimeMillis();
                    while (tmp) {
                        synchronized (result[rank[i] + left]) {
                            if ( result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                result[rank[i] + left] = b[rank[i]];
                                tmp = false;
                            }
                        }
                    }
                    long end=System.currentTimeMillis();
                    time+=end-start;
                    lastFind = rank[i] + left;
                }
            }
        }
    }
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
    static public void check(Long[] result){
        boolean check = true;
        if(result.length!=checking.length){
            System.out.println("length do mot match");
        }
        for (int i = 0; i < checking.length; i++) {
            if (!result[i].equals(checking[i])) {
                check = false;
                //break;
                System.out.println(i + " " + result[i] + " " + checking[i]);
            }
            // System.out.print(result[i-1]+" ");
        }
        if (check)
            System.out.println("checked");
        else
            System.out.println("unchecked");
    }

    public static void main(String[] args) {
        MergeSortTask[] tasks = new MergeSortTask[pnum];
        arr=getArray(beginning,size);
        checking = new Long[size];
        System.arraycopy(arr, 0, checking, 0, size);
        long start = System.currentTimeMillis();
        Arrays.parallelSort(checking);
        long end = System.currentTimeMillis();
        System.out.println("Build in parallel sort cost "+(end-start)+" Millis.");
        start = System.currentTimeMillis();
        int left=0;
        int right=size/pnum;
        int last=size%pnum;
        for (int i = 0; i < pnum; i++) {
            if (last > 0) {
                right++;
                last--;
            }
            tasks[i] = new MergeSortTask(left, right);
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
        end = System.currentTimeMillis();
        System.out.println("distribute into "+pnum+" sub tasks and get "+pnum+" sorted array cost "+(end-start)+" Millis.");
        start = System.currentTimeMillis();
        Long[] rsult= sequenqentMergeOnTheLastStep(tasks);
        end = System.currentTimeMillis();
        System.out.println("Sequent Merge cost "+(end-start)+" Millis.");
        check(rsult);
        start = System.currentTimeMillis();
        for(int i=0;i<pnum;i++){
            que.offer(tasks[i].result);
        }
        int totaltime=0;
        while(que.size()>1){
            a=que.poll();
            b=que.poll();
            int lengthA=a.length, lengthB=b.length;
            result=new Long[lengthA+lengthB];
            Arrays.fill(result, Long.MIN_VALUE);
            int size=lengthA+lengthB;
            MergeTask[] mergeTasks = new MergeTask[pnum];
            right = size / pnum;
            last = size % pnum;
            int aleft = lengthA, bleft = lengthB;
            for (int i = 0; i < pnum; i++) {
                if (last > 0) {
                    right++;
                    last--;
                }
                int[] classes = new int[right];
                int[] ranks = new int[right];
                if (aleft >= right) {
                    Arrays.fill(classes, 0);
                    for (int j = 0; j < right; j++) {
                        ranks[j] = lengthA - aleft;
                        aleft--;
                    }
                } else {
                    if (aleft == 0) {
                        Arrays.fill(classes, 1);
                        for (int j = 0; j < right; j++) {
                            ranks[j] = lengthB - bleft;
                            bleft--;
                        }
                    } else {
                        int tmp = aleft;
                        for (int j = 0; j < tmp; j++) {
                            classes[j] = 0;
                            ranks[j] = lengthA - aleft;
                            aleft--;
                        }
                        for (int j = tmp; j < right; j++) {
                            classes[j] = 1;
                            ranks[j] = lengthB - bleft;
                            bleft--;
                        }
                    }
                }
                mergeTasks[i] = new MergeTask(classes, ranks);
                right = size / pnum;
                mergeTasks[i].start();
            }
            try {
                for (int i = 0; i < pnum; i++) {
                    mergeTasks[i].join();
                }
            } catch (Exception e) {
                System.out.println("Error " + e);
            }
            que.offer(result);
            for(int i=0;i<pnum;i++){
                totaltime+=mergeTasks[i].time;
            }
        }
        result=que.peek();
        end=System.currentTimeMillis();
        System.out.println("finish parallel merge in "+(end-start)+" Millis");
        System.out.println("parallel merge cost synchronized time for "+totaltime+" Millis");


        check(result);
    }
}