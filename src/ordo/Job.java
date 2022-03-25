package ordo;

import config.Project;
import map.*;
import formats.*;
import hdfs.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class Job extends UnicastRemoteObject implements JobInterface {

    public int NumberOfReducers;
    public int NumberOfMaps = 0;
    public Format.Type InputFormat;
    public Format.Type InterFormat;
    public Format.Type OutputFormat;
    public String InputFileName;
    public String InterFileName;
    public String OutputFileName;
    public SortComparator Sortcomparator;
    public JobState state;
    public ArrayList<Worker> workers = new ArrayList<Worker>();



    public Job() throws RemoteException {
        Registry registry;
        try {
            registry = LocateRegistry.getRegistry(Project.job_port);
            registry.rebind("//localhost:" + Project.job_port+ "/Job", this);
            System.out.println("Registry is found.");
        } catch (Exception e) {
            try {
                System.out.println("Creating registry on the port :" + Project.job_port);
                registry = LocateRegistry.createRegistry(Project.job_port);
                registry.rebind("//localhost:" + Project.job_port + "/Job", this);
                System.out.println("Registry created successfully");
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        this.state = JobState.Created;

    }

    public void startJob(MapReduce mr) throws RemoteException {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Initializing the workers

        // Obtaining the number of maps
        this.NumberOfMaps = getMapNumber();
        System.out.println(NumberOfMaps);


        this.state = JobState.Initializing_started;

        // Getting the workers from the registry
        for (int i = 0; i < Project.hosts.length; i++) {
            try {
                Registry registry_work = LocateRegistry.getRegistry(Project.workersports[i]);
                Worker worker = (Worker) registry_work.lookup("//" + Project.hosts[i] + ":" + Project.workersports[i] + "/Worker");
            
                if (worker != null) {
                    this.workers.add(worker);
                }
            
            } catch (Exception e) {
                System.out.println("Can't find Worker");
            }
        }

        // Initialisation du rappel callback
        CallBack callback = null;
        try {
            callback = new CallBackImpl();
        } catch (Exception c) {
            c.printStackTrace();
        }

        this.state = JobState.Initializing_ended;
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Processing Map

        // Launching the workers to process map operation
        this.state = JobState.Map_started;
        for (int i = 0; i < this.NumberOfMaps; i++) {
            // le worker qui effectuera le runmap sur le chunk i
            int workerId = i % Project.hosts.length;
            Worker worker = this.workers.get(workerId);
            Format Tmpinputworker = null;

          /*  if(chunkstates.containsKey(workerId)){
                chunkstates.get(workerId).put(i,ChunkState.Not_processed); 
            }
            else{
                chunkstates.put(workerId, null);
                chunkstates.get(workerId).put(i,ChunkState.Not_processed);
            }
            */

            try {
                if (InputFormat == Format.Type.LINE) {
                    Tmpinputworker = new LineFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-chunk" + i);
                } else if (InputFormat == Format.Type.KV) {
                    Tmpinputworker = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-chunk" + i);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            Format Tmpinterworker = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-res" + i);
            try {
                System.out.println("Lancement du runMap" + i);
                worker.runMap(mr, Tmpinputworker, Tmpinterworker, callback);
                System.out.println("runMap" + i + "Done !");
                              


            } catch (RemoteException r) {
                r.printStackTrace();
            }

        }

        // Attendre que tous les maps aient finit leurs traitements
        try {
            callback.AttendreMaps(NumberOfMaps);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.state = JobState.Map_ended;
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Processing reduce

        this.state = JobState.Reduce_started;

        try {
            HdfsClient.HdfsRead(InputFileName, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Format Outputread = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-res");
        Format Outputreduce = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-final");

        Outputread.open(Format.OpenMode.R);
        Outputreduce.open(Format.OpenMode.W);
        mr.reduce(Outputread, Outputreduce);
        Outputread.close();
        Outputreduce.close();

        this.state = JobState.Reduce_ended;

    }

    public void restartJob(MapReduce mr) throws RemoteException {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Resetting the workers

        System.out.println("Resetting the workers ");
        // Obtaining the number of maps

        this.NumberOfMaps = getMapNumber();
        System.out.println(NumberOfMaps);


        this.state = JobState.Initializing_started;

        // Getting the workers from the registry
        for (int i = 0; i < Project.hosts.length; i++) {
            try {
                Registry registry_work = LocateRegistry.getRegistry(Project.workersports[i]);
                Worker worker = (Worker) registry_work.lookup("//" + Project.hosts[i] + ":" + Project.workersports[i] + "/Worker");
            
                if (worker != null) {
                    this.workers.add(worker);
                }
            
            } catch (Exception e) {
                System.out.println("Can't find Worker");
            }
        }

        // Initialisation du rappel callback
        CallBack callback = null;
        try {
            callback = new CallBackImpl();
        } catch (Exception c) {
            c.printStackTrace();
        }

        this.state = JobState.Initializing_ended;
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Redoing unfinished maps
        System.out.println("Redoing unfinished maps");

        // Relaunching the workers to process map operation
        this.state = JobState.Map_started;
        for (int i = 0; i < this.NumberOfMaps; i++) {
            //the worker which is going to apply the runmap on the chunk i 
            int workerId = i % Project.hosts.length;
            Worker worker = this.workers.get(workerId);

            //if the worker didn't finish map process on chunk i then it redoes it
            if (worker.getWorkerState().equals(WorkerState.On_Map)) {
                Format Tmpinputworker = null;
                try {
                    if (InputFormat == Format.Type.LINE) {
                        Tmpinputworker = new LineFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-chunk" + i);
                    } else if (InputFormat == Format.Type.KV) {
                        Tmpinputworker = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-chunk" + i);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Format Tmpinterworker = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-res" + i);
                try {
                    System.out.println("Lancement du runMap" + i);
                    worker.runMap(mr, Tmpinputworker, Tmpinterworker, callback);
                    System.out.println("runMap" + i + "Done !");
                     

                } catch (RemoteException r) {
                    r.printStackTrace();
                }
            }

        }

        // Attendre que tous les maps aient finit leurs traitements
        try {
            callback.AttendreMaps(NumberOfMaps);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.state = JobState.Map_ended;
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Processing reduce

        System.out.println("Redoing reduce");
        this.state = JobState.Reduce_started;

        try {
            HdfsClient.HdfsRead(InputFileName, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Format Outputread = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-res");
        Format Outputreduce = new KVFormat(HdfsServer.PATH + "tmp/" + InputFileName + "-final");

        Outputread.open(Format.OpenMode.R);
        Outputreduce.open(Format.OpenMode.W);
        mr.reduce(Outputread, Outputreduce);
        Outputread.close();
        Outputreduce.close();

        this.state = JobState.Reduce_ended;

    }

    public int getMapNumber() {
        int nm = 0;
        try {
            NameNode namenode = (NameNode) Naming.lookup(Project.nameNodeURL);

            nm = namenode.getNbrChunks(InputFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nm;

    }

    public void setNumberOfReduces(int tasks) {
        this.NumberOfReducers = tasks;
    }

    public void setNumberOfMaps(int tasks) {
        this.NumberOfMaps = tasks;
    }

    public void setInputFormat(Format.Type ft) throws RemoteException {
        this.InputFormat = ft;
    }

    public void setInputFname(String fname) throws RemoteException {
        this.InputFileName = fname;
    }

    public void setOutputFormat(Format.Type ft) {
        this.OutputFormat = ft;
    }

    public void setOutputFname(String fname) {
        this.OutputFileName = fname;
    }

    public void setSortComparator(SortComparator sc) {
        this.Sortcomparator = sc;
    }

    public int getNumberOfReduces() {
        return this.NumberOfReducers;
    }

    public int getNumberOfMaps() {
        return this.NumberOfMaps;
    }

    public Format.Type getInputFormat() {
        return this.InputFormat;
    }

    public Format.Type getOutputFormat() {
        return this.OutputFormat;
    }

    public String getInputFname() {
        return this.InputFileName;
    }

    public String getOutputFname() {
        return this.OutputFileName;
    }

    public SortComparator getSortComparator() {
        return this.Sortcomparator;
    }

    public JobState getJobState() throws RemoteException{
        return this.state;
    }


    public static void main(String[] args) {
        try {
            new Job();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}