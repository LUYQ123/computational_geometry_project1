
public class ExpParallelMin {
    static long[] arr;

    private static class MinTask extends Thread {
        private long left;
        private long right;
        private long result;

        public MinTask(long left, long right){
            this.left = left;
            this.right = right;
            this.result = Long.MAX_VALUE;
        }

        @Override
        public void run(){
            for (long i = left; i < right; i++) {
                result = arr[(int) i] <result? arr[(int) i]: result;
            }
        }

        public long getResult(){
            return result;
        }
    }


    public static void main(String[] args){
        int size = 1<<27;
        int pnum = 16;

        MinTask tasks[] = new MinTask[pnum];

        arr = new long[size];
        for (int i=0 ; i<arr.length ; i++){
            arr[i]=1;
        }

        arr[0]=2;
        arr[2]=0;
        long start = System.currentTimeMillis();

        for (int i=0 ; i<pnum ; i++) {
            tasks[i] = new MinTask(size/pnum*i,size/pnum*(i+1));
            tasks[i].start();
        }

        try {
            for (int i=0; i<pnum ; i++) {
                tasks[i].join();
            }
        } catch (Exception e){
            System.out.println("Error " + e);
        }

        long endt = System.currentTimeMillis();

        long finalres = Long.MAX_VALUE;
        for (int i=0 ; i<pnum ; i++){
            finalres = tasks[i].getResult()<finalres? tasks[i].getResult() : finalres;
        }

        System.out.println("Min: "+finalres);
        System.out.println("Took "+(endt-start)+ " Millis");

//        long sum=0;
//        start = System.currentTimeMillis();
//
//        for(int i=0;i<size;i++){
//            sum+=1;
//        }
//        endt=System.currentTimeMillis();
//        System.out.println("Sum: "+sum);
//        System.out.println(size);
//        System.out.println("Took "+(endt-start)+ " Millis");
    }
}