package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.Mapper;
import formats.Format;

public interface Worker extends Remote {


	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;

	public int getId() throws RemoteException;

	public int getPort() throws RemoteException;

	public WorkerState getWorkerState() throws RemoteException;

	public void setId(int id_worker) throws RemoteException;

	public void setWorkerState(WorkerState state_worker) throws RemoteException;

	public void actualState() throws RemoteException;

	public Mapper getMapper() throws RemoteException;

	public Format getReader() throws RemoteException;

	public Format getWriter() throws RemoteException;

	public CallBack getCallBack() throws RemoteException;

}
