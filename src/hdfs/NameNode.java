package hdfs;

import java.rmi.*;
import java.util.HashMap;

public interface NameNode extends Remote {
    
    public HashMap<String, Integer> getFile_ChunksMap() throws RemoteException;

    public int getNbrChunks(String hdfsFName) throws RemoteException;

    public void addFile_ChunkToMap(String hdfsFName, int nbrChunks) throws RemoteException;
}
