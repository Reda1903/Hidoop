package ordo;

import map.*;
import formats.*;

import java.net.InetAddress;
import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.Naming;
import config.*;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private int port_worker;
    private int id_worker;
    private WorkerState state_worker;
    private Mapthread thread_worker;
    private static final long serialVersionUID = 1L;
    private Mapper m;
    private Format reader;
    private Format writer;
    private CallBack cb;
    Registry registry;

    public WorkerImpl(int port_worker, int id_worker) throws RemoteException {

        this.port_worker = port_worker;
        this.id_worker = id_worker;

        try {

            registry = LocateRegistry.createRegistry(this.port_worker);
            //String s = InetAddress.getLocalHost().getHostName();
            String s = Project.hosts[id_worker];
            System.out.println(s);
            System.out.println("HostName :" + s);
            System.out.println("Starting the Worker on port " + this.port_worker);
            int index = getIndex();
            if (index == -1) {
                System.out.println("INDEX OF PORT NOT FOUND!");
                return;
            }

            registry.rebind("//" + s + ':' + this.port_worker + "/Worker", this);
            this.state_worker = WorkerState.Created;
            System.out.println("Worker in state: " + displayState());
            System.out.println("id : " + id_worker);
            System.out.println("---------------------------------");

        } catch (Exception e) {
            registry = LocateRegistry.getRegistry(this.port_worker);
            registry.rebind("//" + Project.hosts[id_worker] + ':' + this.port_worker + "/Worker", this);
        }

    }

    private int getIndex() throws RemoteException {
        int index = -1;
        for (int i = 0; i < Project.workersports.length; i++) {
            // System.out.println(Project.workersports[i]);
            if (Project.workersports[i] == this.port_worker) {
                index = i;
                break;
            }
        }
        return index;
    }

    public int getId(){
        return this.id_worker;
    }

    public int getPort(){
        return this.port_worker;
    }

    public WorkerState getWorkerState() throws RemoteException{
        return this.state_worker;
    }

    public void setId(int id_worker){
        this.id_worker = id_worker;
    }

    public void setWorkerState(WorkerState state_worker) throws RemoteException {
        this.state_worker = state_worker;
    }

    public void actualState() {
        try{
            this.setWorkerState(this.thread_worker.State());
        }   
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public String displayState() {
        String displaystate = "";
        if (this.state_worker.equals(WorkerState.Created)){
            displaystate = "Created";
        } 
        else if(this.state_worker.equals(WorkerState.On_Map)){
            displaystate = "Processing a Map";
        }
        else if (this.state_worker.equals(WorkerState.On_Reduce)){
            displaystate = "Finished Map, Processing a Reduce";
        }
        else {
            displaystate = "Unkown State";
        }
        return displaystate;

    }

    public Mapper getMapper() throws RemoteException{
        return this.m;
    }

    public Format getReader() throws RemoteException{
        return this.reader;
    }

    public Format getWriter() throws RemoteException{
        return this.writer;
    }

    public CallBack getCallBack() throws RemoteException{
        return this.cb;
    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

        try {

            Mapthread map_thread = new Mapthread(m, reader, writer, cb);
            this.thread_worker = map_thread;
            this.thread_worker.start();
            this.m = m;
            this.reader = reader;
            this.writer = writer;
            this.cb = cb;
        } catch (RemoteException r) {
            r.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0]);
            int id = Integer.parseInt(args[1]);
            new WorkerImpl(port, id);
        } catch (Exception e) {
            int idf = Integer.parseInt(args[1]);
            System.out.println("Error while creating the worker of ID " + idf);
            e.printStackTrace();
        }

    }

}
