package application;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import config.Project;
import map.MapReduce;
import ordo.Job;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import ordo.JobInterface;
import ordo.JobInterfaceX;

public class MyMapReduce implements MapReduce {
    private static final long serialVersionUID = 1L;

    // MapReduce program that computes word counts
    public void map(FormatReader reader, FormatWriter writer) {

        HashMap<String, Integer> hm = new HashMap<>();
        KV kv;
        while ((kv = reader.read()) != null) {
            String tokens[] = kv.v.split(" ");
            for (String tok : tokens) {
                if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue() + 1);
                else hm.put(tok, 1);
            }
        }
        for (String k : hm.keySet()) writer.write(new KV(k, hm.get(k).toString()));
    }

    public void reduce(FormatReader reader, FormatWriter writer) {
        HashMap<String, Integer> hm = new HashMap<>();
        KV kv;
        while ((kv = reader.read()) != null) {
            if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k) + Integer.parseInt(kv.v));
            else hm.put(kv.k, Integer.parseInt(kv.v));
        }
        for (String k : hm.keySet()) writer.write(new KV(k, hm.get(k).toString()));
    }

    public static void main(String args[]) {
        try {
            //Registry registry = LocateRegistry.getRegistry("calimero.enseeiht.fr", Project.job_port );
            Registry registry = LocateRegistry.getRegistry(Project.job_port);
            JobInterface j = (JobInterface) registry.lookup("//localhost:" + Project.job_port + "/Job");
            j.setInputFormat(Format.Type.LINE);
            j.setInputFname(args[0]);
            long t1 = System.currentTimeMillis();
            j.startJob(new MyMapReduce());
            long t2 = System.currentTimeMillis();
            System.out.println("time in ms =" + (t2 - t1));
            System.exit(0);
        } catch (Exception e) {
            /*try {
                Thread.sleep(2000);
                //Registry registry = LocateRegistry.getRegistry("r2d2.enseeiht.fr", 8686);
                Registry registry = LocateRegistry.getRegistry(Project.job_port);
                JobInterface j = (JobInterface) registry.lookup("//localhost:" + Project.job_port + "/Job");
                j.setInputFormat(Format.Type.LINE);
                j.setInputFname(args[0]);
                long t1 = System.currentTimeMillis();
                j.restartJob(new MyMapReduce());
                long t2 = System.currentTimeMillis();
                System.out.println("time in ms =" + (t2 - t1));
                System.exit(0);
            } catch (InterruptedException | NotBoundException | IOException interruptedException) {
                interruptedException.printStackTrace();
            }*/
            e.printStackTrace();
        }
    }
}