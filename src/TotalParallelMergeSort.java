import java.util.*;

public class TotalParallelMergeSort {
    static Long[] arr;
    static Long[] checking;
    static int pnum = 10;

    static Long[] result;

    static int size=2000;

    static int beginning=0;

    static Queue<Long[]> que= new LinkedList<>();
    static Long[] a;
    static Long[] b;


    private static class MergeTask extends Thread {
        int[] Class;
        int[] rank;
        public MergeTask(int[] Class, int[] rank) {
            this.Class=Class;
            this.rank=rank;
        }

        public void run() {
            Long maxx = Long.max(a[a.length - 1], b[b.length - 1]);
            int lastFind = 0;
            long syn=0;
            //System.out.println(this.getName() + " " + this.Class.length + " tasks distributed");
            for (int i = 0; i < this.Class.length; i++) {
                if (i > 0 && this.Class[i] == this.Class[i - 1] &&
                        ((this.Class[i] == 0 && a[rank[i]].equals(a[rank[i] - 1]) ) ||
                                (this.Class[i] == 1 && b[rank[i]].equals(b[rank[i] - 1])))) {
                    boolean tmp = true;
                    while (lastFind < result.length && tmp) {
                        synchronized (result[lastFind]) {
                            if (result[lastFind] != Long.MIN_VALUE) {lastFind++; syn++;}
                              else {
                                result[lastFind] = this.Class[i] == 0 ? a[rank[i]] : b[rank[i]];
                                tmp = false;
                            }
                        }
                    }
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
                        //System.out.println(left+" "+right);
                    }
                    while (left < b.length && a[rank[i]] > b[left]) left++;
                    boolean tmp = true;
                    while (tmp) {
//                        if((rank[i]+left)==result.length){
//                            String ttmp="";
//                            ttmp+="a:";
//                            for(Long j: a){
//                                ttmp+=" "+j;
//                            }
//                            ttmp+="\n";
//                            ttmp+="b:";
//                            for(Long j: b){
//                                ttmp+=" "+j;
//                            }
//                            ttmp+="\n";
//                            ttmp+="i "+i+" rank[i] "+rank[i]+" left "+left+" a length "+a.length+" b length "+b.length  +"\n class ";
//                            for(int j=0;j<this.Class.length;j++)
//                                ttmp+=this.Class[j]+" ";
//                            ttmp+="\n rank";
//                            for(int j=0;j<this.rank.length;j++)
//                                ttmp+=this.rank[j]+" ";
//                            System.out.println(ttmp);
//                        }
                        synchronized (result[rank[i] + left]) {
                            if (result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                tmp = false;
                            }
                        }
                    }
                    result[rank[i] + left] = a[rank[i]];
                    lastFind = rank[i] + left;
//                    if (a[rank[i]] .equals( maxx)) {
//                        int ttmp = lastFind;
//                        while (ttmp < result.length) result[ttmp++] = a[rank[i]];
//                    }
//                     int t = rank[i] + left;
//                    System.out.print(t+" ");
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
                    while (tmp) {
//                        if((rank[i]+left)==result.length){
//                            String ttmp="";
//                            ttmp+="a:";
//                            for(Long j: a){
//                                ttmp+=" "+j;
//                            }
//                            ttmp+="\n";
//                            ttmp+="b:";
//                            for(Long j: b){
//                                ttmp+=" "+j;
//                            }
//                            ttmp+="\n";
//                            ttmp+="i "+i+" rank[i] "+rank[i]+" left "+left+" a length "+a.length+" b length "+ b.length+"\n class ";
//                            for(int j=0;j<this.Class.length;j++)
//                                ttmp+=this.Class[j]+" ";
//                            ttmp+="\n rank";
//                            for(int j=0;j<this.rank.length;j++)
//                                ttmp+=this.rank[j]+" ";
//                            System.out.println(ttmp);
//                        }
                        if((rank[i] + left)>=result.length)
                            System.out.println("asfasdgvwaf");
                        synchronized (result[rank[i] + left]) {
                            if ( result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                tmp = false;
                            }
                        }
                    }
                    result[rank[i] + left] = b[rank[i]];
                    lastFind = rank[i] + left;
//                    if (b[rank[i]].equals(maxx)) {
//                        int ttmp = lastFind;
//                        while (ttmp < result.length) result[ttmp++] = b[rank[i]];
//                    }
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
    static public Long[] getArray(int beginning, int length) {
        Long[] num = new Long[length];
        num[0] = (long) beginning;
        Random r = new Random(1);
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
                break;
                //System.out.println(i + " " + result[i] + " " + checking[i]);
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
//        for(int i=0;i<pnum;i++){
//            System.out.print("thread "+i+": ");
//            for(int j=0;j<tasks[i].result.length;j++){
//                System.out.print(tasks[i].result[j]+" ");
//            }
//            System.out.println();
//        }

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

//        for(Long[] tmp : que) {
//            for (int i = 0; i < tmp.length; i++) {
//                // System.out.print("(long) "+tmp[i]+", ");
//                System.out.print(tmp[i] + " ");
//            }
//            System.out.println();
//        }
//
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
//            boolean ttmp=false;
//            for(int i=0;i<result.length;i++){
//                if(result[i]==Long.MIN_VALUE){
//                    ttmp=true;
//                    System.out.print(i+" "+ result[i]+" ");
//                    ttmp=true;
//                }
//            }
//            if(ttmp){
//                System.out.print("a ");
//                for(int i=0;i< a.length;i++){
//                    System.out.print(a[i]+ " ");
//                }
//                System.out.println();
//                System.out.print("b ");
//                for(int i=0;i< b.length;i++){
//                    System.out.print(b[i]+ " ");
//                }
//                System.out.println();
//                System.out.print("result ");
//                for(int i=0;i<result.length;i++){
//                    System.out.print(result[i]+" ");
//                }
//                System.out.println();

//                for(int i=0;i< mergeTasks.length;i++){
//                    System.out.print("rank ");
//                    for(int j=0;j<mergeTasks[i].rank.length;j++) {
//                        System.out.print(mergeTasks[i].rank[j] + " ");
//                    }
//                    System.out.println();
//                    System.out.print("class ");
//                    for(int j=0;j<mergeTasks[i].Class.length;j++) {
//                        System.out.print(mergeTasks[i].Class[j] + " ");
//                    }
//                    System.out.println();
//
//                }
//                //System.out.println();
//            }
            //System.out.println(que.size());

        }

        result=que.peek();

        end=System.currentTimeMillis();

        System.out.println("finish parallel merge in "+(end-start)+" Millis");

        check(result);
    }
}