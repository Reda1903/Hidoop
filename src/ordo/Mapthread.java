package ordo;

import java.rmi.RemoteException;
import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;
import ordo.WorkerImpl;
public class Mapthread extends Thread {
    Mapper m;
    Format reader;
    Format writer;
    CallBack cb;
    private WorkerState state;

    public Mapthread(Mapper mapper, Format reader, Format writer, CallBack cb) throws RemoteException{
        this.m = mapper;
        this.reader = reader;
        this.writer = writer;
        this.cb = cb;
        this.state = WorkerState.Created;
    }

    public void run(){

        reader.open(OpenMode.R);
        writer.open(OpenMode.W);
        this.state = WorkerState.On_Map;
        System.out.println("Processing a Map");
        System.out.println("---------------------------------");
        m.map(reader, writer);
        writer.close();
        reader.close();

        try {
            cb.LibererMap();
            this.state = WorkerState.On_Reduce;
            System.out.println("Finished Map, Processing a Reduce");
            System.out.println("---------------------------------");
        } 
        catch( Exception e) {
            e.printStackTrace();
        }

       
    }

    public WorkerState State() {
        return this.state;
    }

}
