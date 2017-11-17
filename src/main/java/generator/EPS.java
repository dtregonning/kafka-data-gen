package generator;

class EPS {
    public static void main(String[] args) {
        EPSToken epsToken = new EPSToken();
        EPSThread thread_01 = new EPSThread("RefreshTokenThread", epsToken);

        EPSThread thread_02 = new EPSThread("WorkerThread01", epsToken);
        EPSThread thread_03 = new EPSThread("WorkerThread02", epsToken);
        EPSThread thread_04 = new EPSThread("WorkerThread03", epsToken);
        EPSThread thread_05 = new EPSThread("WorkerThread04", epsToken);
        EPSThread thread_06 = new EPSThread("WorkerThread05", epsToken);

        try {
            thread_01.thrd.join();
            thread_02.thrd.join();
            thread_03.thrd.join();
            thread_04.thrd.join();
            thread_05.thrd.join();
            thread_06.thrd.join();

        } catch (InterruptedException exc) {
            System.out.println("Thread Interrupted");
        }
    }
}