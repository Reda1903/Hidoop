package ordo;

import map.MapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;

public interface JobInterface extends Remote {
// Méthodes requises pour la classe Job  
	public void setInputFormat(Format.Type ft) throws RemoteException;
    public void setInputFname(String fname) throws RemoteException;

    public void startJob (MapReduce mr) throws RemoteException;
    public void restartJob (MapReduce mr) throws RemoteException;
    public JobState getJobState() throws RemoteException;

}