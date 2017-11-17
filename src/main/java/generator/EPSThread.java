package generator;

class EPSThread implements Runnable {
    Thread thrd;
    EPSToken epsTokenObj;

    EPSThread(String name, EPSToken eps) {
        thrd = new Thread(this, name);
        epsTokenObj = eps;
        thrd.start();
    }

    public void run() {
        boolean finished = false;
        try {
            if (thrd.getName().compareTo("RefreshTokenThread") == 0) {
                for (int i = 0; i < 1000; i++) {
                    epsTokenObj.increaseTokens(1000);
                    Thread.sleep(1000);
                }
                if(epsTokenObj.getTokenCount() == 0) { finished = true;}
            } else {
                do {
                    if (epsTokenObj.takeToken()) {
                        System.out.println(thrd.getName() + " Success");
                    }
                } while(finished == false);
            }
        } catch (InterruptedException exc) {
            System.out.println("Thread Interrupted");
        }
    }

}