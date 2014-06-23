/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prismadriver;

/**
 *
 * @author kgot
 */
public class Main {

    /**
     * @param args the command line arguments
     *
     *
     * 3 filename
     */
    public static void main(String[] args) {

        System.out.println("PrismaDriver version 1.2");
        String hostname = args[0]; // localhost, 192.168.23.2, etc
        int port = Integer.parseInt(args[1]);  // tcp port

        // sql mode
        if (args.length == 4) {
            PrismaDataHandler client = new PrismaDataHandler(args[2], args[3]);
            client.DataReader(hostname, port);
            
        } 
        // text mode
        else if (args.length == 3) {
            PrismaDataHandler client = new PrismaDataHandler(args[2]);
            client.DataReader(hostname, port);
        } else {
            
            System.out.println("Wrong number of arguments.");
        }
    }

}
