import java.util.Arrays;
import java.util.Random;

public class ParallelMerge {

    static Long[] result;

    static Long[] a;
    static Long[] b;

    static int pnum = 23;

    private static class Merge extends Thread {
        int[] Class;
        int[] rank;
        public Merge(int[] Class, int[] rank) {
            this.Class=Class;
            this.rank=rank;
        }

        public void run() {
            for (int i = 0; i < this.Class.length; i++) {
                if (this.Class[i] == 0) {
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
                        synchronized (result[rank[i] + left]) {
                            if (result[rank[i] + left] != Long.MIN_VALUE) {
                                left++;
                            } else {
                                tmp = false;
                            }
                        }
                    }
                    result[rank[i] + left] = a[rank[i]];
                   // int t = rank[i] + left;
                    //System.out.print(t+" ");
                }
                if (this.Class[i] == 1) {
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
                   // int t = rank[i] + left;
                }
            }
        }
    }

    static public Long[] getArray(int beginning, int length){
        Long[] num=new Long[length];
        num[0]= (long) beginning;
        Random r=new Random();
        for(int i=1;i<length;i++){
            num[i]=num[i-1]+r.nextInt(2);
        }
        return num;
    }

    public static void main(String[] args) {
        int lengthA = 1011000, lengthB = 100000;
        int beginningA = 0, beginningB = 0;
        int size=lengthA+lengthB;
        a = getArray(beginningA, lengthA);
        b = getArray(beginningB, lengthB);
        result = new Long[size];
        Arrays.fill(result,Long.MIN_VALUE);

//        System.out.print("Array A: ");
//        for(int i=0;i<lengthA;i++){
//            System.out.print(a[i]+" ");
//        }
//        System.out.println();
//        System.out.print("Array B: ");
//        for(int i=0;i<lengthB;i++){
//            System.out.print(b[i]+" ");
//        }
//        System.out.println();

        Merge[] tasks = new Merge[pnum];
        long start = System.currentTimeMillis();

        int right=size/pnum;
        int last=size%pnum;
        int aleft=lengthA, bleft=lengthB;
        for (int i = 0; i < pnum; i++) {
            if (last > 0) {
                right++;
                last--;
            }
            int[] classes=new int[right];
            int[] ranks=new int[right];
            if(aleft>=right){
                Arrays.fill(classes,0);
                for(int j=0;j<right;j++){
                    ranks[j]=lengthA-aleft;
                    aleft--;
                }
            } else {
                if(aleft==0){
                    Arrays.fill(classes,1);
                    for(int j=0;j<right;j++){
                        ranks[j]=lengthB-bleft;
                        bleft--;
                    }
                }
                else{
                    int tmp=aleft;
                    for(int j=0;j<tmp;j++){
                        classes[j]=0;
                        ranks[j]=lengthA-aleft;
                        aleft--;
                    }
                    for(int j=tmp;j<right;j++){
                        classes[j]=1;
                        ranks[j]=lengthB-bleft;
                        bleft--;
                    }
                }
            }
            tasks[i] = new Merge(classes, ranks);
            //  System.out.println(left+"  "+right);
            right = size / pnum;
            tasks[i].start();
        }

        try {
            for (int i = 0; i < pnum; i++) {
                tasks[i].join();
            }
        } catch (Exception e) {
            System.out.println("Error " + e);
        }
        System.out.print("result: ");
        boolean check=true;
        for(int i=1;i<lengthA+lengthB;i++){
            if(result[i]<result[i-1])   {
                check=false;
                break;
            }
            //System.out.print(result[i-1]+" ");
        }
       // System.out.println(result[result.length-1]);
        if(check)
            System.out.println("checked");
        else
            System.out.println("unchecked");
    }
}
