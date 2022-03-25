package hdfs;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.HashMap;
import config.Project;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode {
    private HashMap<String, Integer> file_ChunksMap;
    private static final long serialVersionUID = 1L;
    
    protected NameNodeImpl() throws RemoteException {
        file_ChunksMap = new HashMap<String, Integer>();
    }

    @Override
    public HashMap<String, Integer> getFile_ChunksMap() throws RemoteException {
        return file_ChunksMap;
    }

    @Override
    public int getNbrChunks(String hdfsFName) throws RemoteException {
        if (file_ChunksMap.containsKey(hdfsFName)){
            final int nbrChunks = file_ChunksMap.get(hdfsFName);
            System.out.println("Found file " + hdfsFName + " and its " + nbrChunks + " chunks in NameNode...");
            System.out.println("------------------------------------");
            return nbrChunks;
        }
        System.out.println("Could not Find file " + hdfsFName + " in NameNode...");
        System.out.println("------------------------------------");
        return 0;
    }
    
    @Override
    public void addFile_ChunkToMap(String hdfsFName, int nbrChunks) throws RemoteException{
        if (nbrChunks <= 0) return;

        if(file_ChunksMap.containsKey(hdfsFName)){
            file_ChunksMap.remove(hdfsFName);
            System.out.println("Removed existing file " + hdfsFName + " and its " + file_ChunksMap.get(hdfsFName) + " chunks from NameNode...");
        }
        file_ChunksMap.put(hdfsFName, nbrChunks);
        System.out.println("Added file " + hdfsFName + " and its " + nbrChunks + " chunks to NameNode...");
        System.out.println("------------------------------------");
    }
    public static void main(String args[]) {
        try {
            // Create an instance of the server object
            NameNodeImpl nameNode = new NameNodeImpl();
            // Create the registry
            try {
                Registry registry = LocateRegistry.createRegistry(4000);
            } catch (Exception ex) {
                System.out.println(" The registry is already running...");
            }
            // Register the object with the naming service
            Naming.rebind(Project.nameNodeURL, nameNode);
            System.out.println("NameNode " + "bound in registry...");
            System.out.println("Waiting for command...");
            System.out.println("------------------------------------");
        } catch (Exception exc) {		
            System.out.println("Error when instantiating NameNode");          
            exc.printStackTrace(); }
         
    }
}
