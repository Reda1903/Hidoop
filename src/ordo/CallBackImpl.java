package ordo;

import java.rmi.*;
import java.rmi.server.*;
import java.util.concurrent.Semaphore;


public class CallBackImpl extends UnicastRemoteObject implements CallBack {
    /**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private Semaphore barriere;
    
    public CallBackImpl() throws RemoteException{
        barriere = new Semaphore(0);
    }

    public void LibererMap() throws RemoteException{
        barriere.release();
    }
    
    public void AttendreMaps(int nbmaps) throws RemoteException{
        for (int i = 0; i<nbmaps; i++){
            try  {
                barriere.acquire();
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }

        }
    }

}
