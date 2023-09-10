import java.util.Arrays;
import java.util.Random;

public class test {
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
        int lengthA = 10, lengthB = 10;
        int beginningA = 0, beginningB = 0;
//        Long[] a = getArray(beginningA, lengthA);
//        Long[] b = getArray(beginningB, lengthB);
        Long[] a=new Long[]{(long)0, (long)0,(long) 0, (long)0, (long)1, (long)2,(long) 3,(long) 4,(long) 5,(long) 5 };
        Long[] b=new Long[]{(long)0, (long)1,(long) 2, (long)3, (long)3, (long)4,(long) 4,(long) 5,(long) 6,(long) 6 };
        Long[] result = new Long[lengthA + lengthB];
        Arrays.fill(result,(long)-1);
        System.out.print("Array A: ");
        for(int i=0;i<lengthA;i++){
            System.out.print(a[i]+" ");
        }
        System.out.println();
        System.out.print("Array B: ");
        for(int i=0;i<lengthB;i++){
            System.out.print(b[i]+" ");
        }
        System.out.println();
        for (int i = 0; i < lengthA; i++) {
            int left = 0;
            int right = b.length - 1;
            int mid=0;
            while (right > left) {
                 mid = (right + left) / 2;
                if (a[i] < b[mid]) {
                    right = mid-1;
                } else if (a[i] > b[mid]) {
                    left = mid+1;
                }
                else{
                    left=mid;
                    right=mid;
                }
                //System.out.println(left+" "+right);
            }
            while (left>0 && a[i]<=b[left-1])   left--;
            if(a[i]>b[b.length-1])    left++;
            while(result[i+left]!=-1){  left++; }
            result[i+left]=a[i];
            int t=i+left;
            System.out.print(t+" ");
        }
        System.out.println();
        for (int i = 0; i < lengthB; i++) {
            int left = 0;
            int right = a.length - 1;
            while (right > left) {

                int mid = (right + left) / 2;
                if (b[i] <= a[mid]) {
                    right = mid-1;
                } else if (b[i] > a[mid]) {
                    left = mid+1;
                }else{
                    left=mid;
                    right=mid;
                }
            }
            while (left>0 && b[i]<=a[left-1])   left--;
            if(b[i]>a[a.length-1])    left++;
            while(result[i+left]!=-1){  left++; }
            result[i+left]=b[i];
            int t=i+left;
            System.out.print(t+" ");
        }
        System.out.println();

        System.out.print("result: ");
        for(int i=0;i<lengthA+lengthB;i++){
            System.out.print(result[i]+" ");
        }
        System.out.println();
    }
}
