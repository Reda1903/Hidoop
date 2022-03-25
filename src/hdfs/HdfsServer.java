package hdfs;

import java.io.*;
import java.net.*;
import formats.Format;

public class HdfsServer {

    private static boolean isRunning = true;
    public static String PATH = System.getProperty("user.dir").substring(0,
            System.getProperty("user.dir").length() - 3);

    public static void main(String[] args) {
        //System.out.println(PATH);
        if (args.length == 0)
            return;

        final int port = Integer.parseInt(args[0]);

        try {
            ServerSocket serverSocket = new ServerSocket(port);

            while (getIsRunning()) {
                System.out.println("------------------------------------");
                System.out.println("Waiting for Commands on Port : " + port + "...");
                Socket socket = serverSocket.accept();
                
                Command receivedCommand = (Command) (new ObjectInputStream(socket.getInputStream()).readObject());
                String fileName = receivedCommand.getFileName();
                Format.Type fmt = receivedCommand.getFormat();
                String format = "";
                if(fmt != null) format = fmt.toString();

                System.out.println("Command Received : " + receivedCommand.getCommandType().toString() + " " + format + " "
                        + fileName + "...");
                
                //TO DO: UNCOMMENT
                switch (receivedCommand.getCommandType()) {
                    case CMD_DELETE:
                        deletChunk(fileName);
                        break;
                    case CMD_READ:
                        readChunk(fileName, socket);
                        break;
                    case CMD_WRITE:
                        writeChunk(fileName, receivedCommand.getFileContent());
                        break;
                    default:
                        break;

                }
            }
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void deletChunk(String fileName) {
        File fileToDelete = new File(PATH + "tmp/" + fileName);
        fileToDelete.delete();
        System.out.println("Chunk " + fileName + " has been deleted ...");
    }

    private static void readChunk(String fileName, Socket socket) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        final File fileToRead = new File(PATH + "tmp/" + fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileToRead));
        
        String chunkLine = bufferedReader.readLine();
        String chunk = "";
        while (chunkLine != null){
            chunk += chunkLine + "\n";
            chunkLine = bufferedReader.readLine();
        }
        bufferedReader.close();

        //Send read chunk back to client
        //TO DO: UNCOMMENT
        oos.writeObject(chunk);
        oos.close();
        System.out.println("Chunk " + fileName + " has been sent back to client ...");
    }

    private static void writeChunk(String fileName, String fileContent) throws IOException {
        if(fileContent == null) return;

        final File fileToWrite = new File(PATH + "tmp/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileToWrite));
        bufferedWriter.write(fileContent);
        bufferedWriter.close();
        System.out.println("Chunk " + fileName + " has been written ...");
    }

    public static boolean getIsRunning() {
        return isRunning;
    }

    public void setRunning(boolean isRunning) {
        HdfsServer.isRunning = isRunning;
    }
}