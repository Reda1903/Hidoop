package ordo;

import java.rmi.*;


public interface CallBack extends Remote {

    public void LibererMap() throws RemoteException;

    public void AttendreMaps(int cpt) throws RemoteException;
    
}