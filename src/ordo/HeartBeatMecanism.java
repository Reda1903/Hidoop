package ordo;

import java.net.ConnectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.File;

import config.*;
import formats.*;
import map.*;

public class HeartBeatMecanism {

    public static void main(String[] args) {

        try {
            while (true) {
                Registry registry;
                try {
                    registry = LocateRegistry.getRegistry(Project.job_port);
                    JobInterface j = (JobInterface) registry.lookup("//localhost:" + Project.job_port + "/Job");
                    if (j.getJobState() == JobState.Created) {
                        System.out.println("Server Job is working");
                    } else if (j.getJobState() == JobState.Initializing_started) {
                        System.out.println("Server Job initializing started successfully");
                    } else if (j.getJobState() == JobState.Initializing_ended) {
                        System.out.println("Server Job initializing finished successfully");
                    } else if (j.getJobState() == JobState.Map_started) {
                        System.out.println("Server Job Map started successfully");
                    } else if (j.getJobState() == JobState.Map_ended) {
                        System.out.println("Server Job Map finished successfully");
                    } else if (j.getJobState() == JobState.Reduce_started) {
                        System.out.println("Server Job Reduce started successfully");
                    } else if (j.getJobState() == JobState.Reduce_ended) {
                        System.out.println("Server Job Reduce finished successfully");
                    }
                } catch (Exception e) {
                    // 1 - If Job Server goes down
                    System.out.println("Server Job is down");
                    System.out.println("Restarting Job Server on Port : " + Project.job_port);
                    System.out.println("---------------------------------------------------");

                    //Runtime.getRuntime().exec("./restarting_job.sh");
                    
                    String []command ={"/bin/bash","-c", "java ordo.Job"};
                    Process exec = Runtime.getRuntime().exec(command,null);                  

                    /*
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command("/home/ftoubali/Bureau/S8/Hidoop_S8_v2/hidoopfonctionnel/src/ordo/restarting_job.sh");
                    Process process = processBuilder.start();
                    */

                    Thread.sleep(3000);
                }

                Thread.sleep(1000);
                
                // 2 - If a worker goes down
                for (int i = 0; i < Project.workersports.length; i++) {
                    Registry registry2;
                    try {
                        registry2 = LocateRegistry.getRegistry(Project.workersports[i]);
                        Worker w = (Worker) registry2
                                .lookup("//" + Project.hosts[i] + ":" + Project.workersports[i] + "/Worker");
                        if (w.getWorkerState() == WorkerState.Created) {
                            System.out.println("Worker " + w.getId() + " is up and initialized");
                        } else if (w.getWorkerState() == WorkerState.On_Map) {
                            System.out.println("Worker " + w.getId() + " is working on a map");
                        } else if (w.getWorkerState() == WorkerState.On_Reduce) {
                            System.out.println("Worker " + w.getId() + " is working on a map");
                        }

                    } catch (Exception e) {
                        // 1 - If Job Server goes down
                        System.out.println("worker " + i +"is down");
                        System.out.println("Restarting worker on Port : " + Project.workersports[i]);
                        System.out.println("---------------------------------------------------");

                        String []command ={"/bin/bash","-c", "java ordo.WorkerImpl " +  Project.workersports[i] + " " + i};
                        Process exec = Runtime.getRuntime().exec(command);  
                        Thread.sleep(1000);

                        System.out.println("Worker is restarted on Port : " + Project.workersports[i]);

                        registry2 = LocateRegistry.getRegistry(Project.workersports[i]);
                        Worker w = (Worker) registry2
                                .lookup("//" + Project.hosts[i] + ":" + Project.workersports[i] + "/Worker");
                        System.out.println(" The registry of the Worker : " + Project.workersports[i] + " is found");


                        if (w.getWorkerState().equals(WorkerState.On_Map)) {
                            System.out.println("Restarting runMap of the worker on Port : " + Project.workersports[i]);
                            Mapper m = w.getMapper();
                            Format reader = w.getReader();
                            Format writer = w.getWriter();
                            CallBack cb = w.getCallBack();

                            w.runMap(m,reader,writer,cb);


                        }
                        /*
                        String []cmd = new String[3];
                        cmd[0] = "./restarting_worker.sh";
                        cmd[1] = String.valueOf(Project.workersports[i]);
                        cmd[2] = String.valueOf(i);
                        */
                        /*
                        Process restarting_worker = Runtime.getRuntime().exec("./restarting_worker.sh" + Project.workersports[i] + " " + i);
                        restarting_worker.waitFor();
                        */
                    }
                }                        
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}