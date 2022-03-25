package config;
import java.util.HashMap;

public class Project {
    
    //Hosts des workers
    public static int job_port = 8686;

    //ports HDFSSERVER
    public static int ports[] = {8001, 8002, 8003};

    //public static String nameNodeURL = "//targaryen.enseeiht.fr:4000/NameNode";
    public static String nameNodeURL = "//localhost:4000/NameNode";

    //public static String hosts[] = {"r2d2.enseeiht.fr", "hydre.enseeiht.fr", "pikachu.enseeiht.fr"};
    public static String hosts[] = {"localhost", "localhost", "localhost"};

    //public static String workershosts[] = {"r2d2.enseeiht.fr", "nickel.enseeiht.fr", "gandalf.enseeiht.fr"};

    //Ports des workers
    public static int workersports [] = {8881,8882,8883};
    
}
