import java.util.Arrays;
import java.util.Random;

public class ParallelMerge {

    static Long[] result;
    static Long[] a;
    static Long[] b;
    static int pnum = 50;
    static int lengthA = 100000;
    static int lengthB = 10000000;
    static long beginningA = 0;
    static long beginningB = -5850;

    private static class Merge extends Thread {
        int[] Class;
        int[] rank;

        public Merge(int[] Class, int[] rank) {
            this.Class = Class;
            this.rank = rank;
        }

        public void run()
        {
            //Long maxx = Long.max(a[a.length - 1], b[b.length - 1]);
            int lastFind = 0;
            //long syn=0;
            //System.out.println(this.getName() + " " + this.Class.length + " tasks distributed");
            for (int i = 0; i < this.Class.length; i++) {
                if (i > 0 && this.Class[i] == this.Class[i - 1] &&
                        ((this.Class[i] == 0 && a[rank[i]].equals(a[rank[i] - 1])) ||
                                (this.Class[i] == 1 && b[rank[i]].equals(b[rank[i] - 1])))) {
                    boolean tmp = true;
                    while (lastFind < result.length && tmp) {
                        synchronized (result[lastFind]) {
                            if (result[lastFind] != Long.MIN_VALUE) lastFind++;
                            else {
                                result[lastFind] = this.Class[i] == 0 ? a[rank[i]] : b[rank[i]];
                                tmp = false;
                            }
                        }
                    }
                } else if (this.Class[i] == 0) {
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
                    while (tmp) {
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
                } else if (this.Class[i] == 1) {
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
                        synchronized (result[rank[i] + left]) {
                            if (result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                tmp = false;
                            }
                        }
                    }
                    result[rank[i] + left] = b[rank[i]];
                    lastFind = rank[i] + left;
                }
            }
        }
    }

    static public Long[] getArray(long beginning, int length) {
        Long[] num = new Long[length];
        num[0] =  beginning;
        Random r = new Random(1);
        for (int i = 1; i < length; i++) {
            int tmp = r.nextInt(10);
            if (tmp < 8) num[i] = num[i - 1];
            else num[i] = num[i - 1] + r.nextInt(4);
        }
        return num;
    }

    public static void main(String[] args) {

        int size = lengthA + lengthB;
        a = getArray(beginningA, lengthA);
        b = getArray(beginningB, lengthB);
        result = new Long[size];
        Arrays.fill(result, Long.MIN_VALUE);

        System.out.println("finish generate array");
        Merge[] tasks = new Merge[pnum];
        long start;
        int right = size / pnum;
        int last = size % pnum;
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
            tasks[i] = new Merge(classes, ranks);
            right = size / pnum;
            tasks[i].start();
        }
        System.out.println("finish distribute tasks");
        start = System.currentTimeMillis();
        try {
            for (int i = 0; i < pnum; i++) {
                tasks[i].join();
            }
        } catch (Exception e) {
            System.out.println("Error " + e);
        }
        Long[] checking = new Long[lengthA + lengthB];
        System.arraycopy(a, 0, checking, 0, lengthA);
        System.arraycopy(b, 0, checking, lengthA, lengthB);
        Arrays.parallelSort(checking);
        boolean check = true;
        for (int i = 0; i < lengthA + lengthB; i++) {
            if (!result[i].equals(checking[i])) {
                check = false;
                System.out.println(i + " " + result[i] + " " + checking[i]);
            }
        }
        if (check)
            System.out.println("checked");
        else
            System.out.println("unchecked");
        long end= System.currentTimeMillis();
        System.out.println("Finish in "+(end-start) +  " Millis");
    }
}
