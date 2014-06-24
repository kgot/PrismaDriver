/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prismadriver;

import java.io.*;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class reads the packages from quax modules, parses the data and upadtes the database.
 *
 * @author kgot
 */
public class PrismaDataHandler {

    String filename = "prismadata.txt";
    boolean TEXT_MODE = false; // set true for text storing
    boolean SQL_MODE = false; // set true for database storing
    boolean DEBUG = true;
    // sql vars
    String username = "root";
    String password = "";
    String url = "jdbc:mysql://localhost:3306/awesomedb";
    String sensor;
    String date;
    String time;
    String milis;
    double humidity;
    double temperature;
    double light;
    // time threshold in milliseconds
    int idleThreshold = 10000;
    //
    // - ASCII CODES -
    //
    // CHAR | MEANING          | HEX | DEC 
    // SYN  | synchronous idle | 16  | 022 
    // STX  | start of text    | 02  | 002 
    // DLE  | data link escape | 10  | 016 
    // ETX  | end of text      | 03  | 003
    //
    String pkg_state = "";  // { S_SYN, S_STX, S_DLE, S_LENGTH, S_DATA, S_ETX }
    private byte[] new_pkg = new byte[120];
    private short new_pkg_length = 0;
    private short new_pkg_pointer = 0;
    private byte[] pkg_data = null;
    private short pkg_length = 0;
    private byte pkg_type = 0;
    private int pkg_addh = 0;
    private int pkg_addl = 0;
    private String packetdata = "";
    // DEC CODES
    private int D_SYN = 022;
    private int D_STX = 002;
    private int D_DLE = 016;
    private int D_ETX = 003;
    // HEX CODES
    private byte H_SYN = 0x16;
    private byte H_STX = 0x02;
    private byte H_DLE = 0x10;
    private byte H_ETX = 0x03;

    /**
     *
     */
    public PrismaDataHandler() {
    }

    /**
     *
     * @param filename
     */
    public PrismaDataHandler(String filename) {
        TEXT_MODE = true;
        this.filename = filename;
    }

    /**
     *
     * @param filename
     */
    public PrismaDataHandler(String sqlmode, String url) {
        if (sqlmode.equals("sql")) {
            SQL_MODE = true;
            this.url = url;
        }
    }

    /**
     *
     * @param hostname
     * @param port
     */
    public void getRawData(String hostname, int port) {
        try {
            // tcp client - reads data from gateway
            Socket socket = new Socket(hostname, port);
            System.out.println("Connected to " + socket.getInetAddress() + " on port " + socket.getPort());
            System.out.println("Received data: ");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            char[] input = new char[4096];
            byte[] input_buf = new byte[4096];

            in.read(input, port, port);


            int len;
            byte buffer[] = new byte[4096];

        } catch (UnknownHostException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     *
     * @param hostname
     * @param port
     */
    public void DataReader(String hostname, int port) {
        try {
            // ping gateway's ip
            InetAddress inet = InetAddress.getByName(hostname);
            System.out.println("Sending Ping Request to " + hostname);

            boolean status = inet.isReachable(5000); //Timeout = 5000 milli seconds

            if (status) {

                // tcp client - reads data from gateway
                Socket socket = new Socket(hostname, port);
                System.out.println("Connected to " + socket.getInetAddress() + " on port " + socket.getPort());

                DataInputStream dis = new DataInputStream(socket.getInputStream());

                byte[] input = new byte[512];
                byte[] inputarray = null;
                String[] packetcontent = new String[2];
                byte b;
                int j = 0;

                // data reading loop
                while (true) {
                    b = dis.readByte();
                    input[j] = b;

                    if (DEBUG) {
                        String str = String.format("%X", b);
                        System.out.print(str);
                    }

                    if (b == H_ETX) {
                        // new byte array with packet length
                        inputarray = new byte[j + 2];

                        // copy packet bytes to array with fixed length
                        for (int i = 0; i < inputarray.length - 1; i++) {
                            inputarray[i] = input[i];
                        }

                        // next byte is SYN code
                        inputarray[j + 1] = dis.readByte(); // read SYN
                        j = 0;

                        // extract packet information
                        packetcontent = packetProcessing(inputarray);

                        parseData(packetcontent);

                    }

                    j++;
                }

            } else {
                System.err.println("ERROR: Cannot connect to PRISMA Gateway. IP is not reachable");
                System.out.println("The program will try to reconnect in a few seconds...");
                try {
                    // wait for 5 seconds
                    Thread.sleep(5000);
                } catch (InterruptedException ex1) {
                    Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
                }
                // try to reconnect
                DataReader(hostname, port);
            }
        } catch (ConnectException ex) {
            System.err.println("ERROR: Cannot connect to PRISMA Gateway");
            System.out.println("The program will try to reconnect in a few seconds...");
            try {
                // wait for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException ex1) {
                Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
            }
            // try to reconnect
            DataReader(hostname, port);
        } catch (java.net.NoRouteToHostException ex) {
            System.err.println("ERROR: Not connected to PRISMA WiFi");
            System.out.println("The program will try to reconnect in a few seconds...");
            try {
                // wait for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException ex1) {
                Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
            }
            // try to reconnect
            DataReader(hostname, port);
        } catch (StackOverflowError ex5) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex5);
            DataReader(hostname, port);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            try {
                // wait for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException ex1) {
                Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
            }
            // try to reconnect
            DataReader(hostname, port);
        }
    }

    /**
     * Old method, not used. Doesn't parse sensor ID.
     *
     * @param hostname
     * @param port
     */
    public void PrismaDataRead(String hostname, int port) {

        try {
            // ping gateway's ip
            InetAddress inet = InetAddress.getByName(hostname);
            System.out.println("Sending Ping Request to " + hostname);

            boolean status = inet.isReachable(5000); //Timeout = 5000 milli seconds

            if (status) {
                // tcp client - reads data from gateway
                Socket socket = new Socket(hostname, port);
                System.out.println("Connected to " + socket.getInetAddress() + " on port " + socket.getPort());

                System.out.println("Received data: ");

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                double curstarttime = Calendar.getInstance().getTimeInMillis();

                int c;

                // parsing sensor data & extracting variables
                while ((c = in.read()) != -1) {

                    // check if there is streaming of data
                    double curendtime = Calendar.getInstance().getTimeInMillis();
                    if ((curendtime - curstarttime) > idleThreshold) {
                        in.close();
                        socket.close();
                        System.out.println("No recent data received.");
                        System.out.println("The program will try to reconnect in a few seconds...");
                        PrismaDataRead(hostname, port);
                        break;
                    }

                    if (DEBUG) {
                        System.out.print((char) c);
                    }

                    if (((char) c) == 'L') { // building string for LIGHT variable
                    } // building string for TEMPERATURE variable
                    else if (((char) c) == 'T') {
                        c = in.read();

                        if (((char) c) == '=') {
                            // do nothing
                            // its Tvar which is not used
                        } else {
                            //getVar("Temperature", in);
                        }

                    } // building string for HUMIDITY variable
                    else if (((char) c) == 'H') {
                        //getVar("Humidity", in);
                    }

                }

                in.close();
                socket.close();

            } else {
                System.err.println("ERROR: Cannot connect to PRISMA Gateway. IP is not reachable");
                System.out.println("The program will try to reconnect in a few seconds...");
                try {
                    // wait for 5 seconds
                    Thread.sleep(5000);
                } catch (InterruptedException ex1) {
                    Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
                }
                // try to reconnect
                PrismaDataRead(hostname, port);
            }



        } catch (ConnectException ex) {
            System.err.println("ERROR: Cannot connect to PRISMA Gateway");
            System.out.println("The program will try to reconnect in a few seconds...");
            try {
                // wait for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException ex1) {
                Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
            }
            // try to reconnect
            PrismaDataRead(hostname, port);
        } catch (java.net.NoRouteToHostException ex) {
            System.err.println("ERROR: Not connected to PRISMA WiFi");
            System.out.println("The program will try to reconnect in a few seconds...");
            try {
                // wait for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException ex1) {
                Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex1);
            }
            // try to reconnect
            PrismaDataRead(hostname, port);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param textline
     */
    public void WriteToText(String textline) {
        try {
            PrintWriter tout = new PrintWriter(new FileWriter(filename, true));

            tout.println(textline);

            tout.close();
        } catch (IOException ex) {
            System.out.println("Write text to file failed.");
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     *
     * @param ddate
     * @param ttime
     * @param milis
     * @param data
     * @param type
     */
    public void WriteToDB(String sensor, String ddate, String ttime, String milis, double data, String type) {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = DriverManager.getConnection(url, username, password);
            Statement stmt = con.createStatement();
            System.out.println("Database connection established");

            //IF not exists
            //CREATE DB
            try {
                //TODO it reutrns 1? what does it reutrn

                if (stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS awesomedb") != 1) {
                    System.err.println("Create Database failed.");
                }

            } catch (SQLException s) {
                System.out.println("Couldn't Create DB!");
                s.printStackTrace();
            }

            // connect to DB
            try {
                con = DriverManager.getConnection(url, username, password);
                stmt = con.createStatement();
            } catch (SQLException s) {
                System.out.println("Couldn't Connect to DB!");
                s.printStackTrace();
            }

            //IF not exists
            //CREATE TABLE
            try {
                stmt = con.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS awesomedb.prismahumidity ("
                        + "sensorID VARCHAR(30) NULL DEFAULT '0' ,"
                        + "date DATE NULL DEFAULT NULL ,"
                        + "time TIME NULL DEFAULT NULL ,"
                        + "milliseconds INT NULL DEFAULT NULL,"
                        + "humidity DOUBLE NULL ,"
                        + "PRIMARY KEY ( sensorID , date , time , milliseconds ))");
            } catch (SQLException s) {
                System.out.println("Couldn't Create Table!");
                s.printStackTrace();
            }

            //IF not exists
            //CREATE TABLE
            try {
                stmt = con.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS awesomedb.prismatemperature ("
                        + "sensorID VARCHAR(30) NULL DEFAULT '0' ,"
                        + "date DATE NULL DEFAULT NULL ,"
                        + "time TIME NULL DEFAULT NULL ,"
                        + "milliseconds INT NULL DEFAULT NULL,"
                        + "temperature DOUBLE NULL DEFAULT NULL ,"
                        + "PRIMARY KEY ( sensorID, date , time , milliseconds))");
            } catch (SQLException s) {
                System.out.println("Couldn't Create Table!");
                s.printStackTrace();
            }

            //IF not exists
            //CREATE TABLE
            try {
                stmt = con.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS awesomedb.prismalight ("
                        + "sensorID VARCHAR(30) NULL DEFAULT '0' ,"
                        + "date DATE NULL DEFAULT NULL ,"
                        + "time TIME NULL DEFAULT NULL ,"
                        + "milliseconds INT NULL DEFAULT NULL,"
                        + "light DOUBLE NULL DEFAULT NULL ,"
                        + "PRIMARY KEY ( sensorID, date , time , milliseconds))");
            } catch (SQLException s) {
                System.out.println("Couldn't Create Table!");
                s.printStackTrace();
            }

            // write data to db
            if (type.equals("h")) {
                try {
                    stmt = con.createStatement();
                    stmt.execute("INSERT INTO prismahumidity (sensorID, date, time, milliseconds, humidity) VALUES ('"
                            //INSERT date from message (date of message formed)
                            + sensor + "', '"
                            + ddate + "', '"
                            + ttime + "', '"
                            + milis + "', '"
                            + data + "'"
                            + ")");
                    System.out.println("Insert executed.");
                } catch (SQLException s) {
                    System.out.println("Couldn't insert data");
                    s.printStackTrace();
                }
            }

            if (type.equals("t")) {
                try {
                    stmt = con.createStatement();
                    stmt.execute("INSERT INTO prismatemperature (sensorID, date, time, milliseconds, temperature) VALUES ('"
                            //INSERT date from message (date of message formed)
                            + sensor + "', '"
                            + ddate + "', '"
                            + ttime + "', '"
                            + milis + "', '"
                            + data + "'"
                            + ")");
                    System.out.println("Insert executed.");
                } catch (SQLException s) {
                    System.out.println("Couldn't insert data");
                    s.printStackTrace();
                }
            }

            if (type.equals("l")) {
                try {
                    stmt = con.createStatement();
                    stmt.execute("INSERT INTO prismalight (sensorID, date, time, milliseconds, light) VALUES ('"
                            //INSERT date from message (date of message formed)
                            + sensor + "', '"
                            + ddate + "', '"
                            + ttime + "', '"
                            + milis + "', '"
                            + data + "'"
                            + ")");
                    System.out.println("Insert executed.");
                } catch (SQLException s) {
                    System.out.println("Couldn't insert data");
                    s.printStackTrace();
                }
            }

        } catch (InstantiationException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PrismaDataHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.out.println("Database connection terminated.");
            }
        }
    }

    /**
     *
     * @param mode
     * @param in
     */
    public void parseData(String[] stra) {

        String address = stra[0];
        String data = stra[1];
        String mode = "";
        char[] strarray = data.toCharArray();
        char c = ' ';

        StringBuilder line = new StringBuilder();

        Calendar currentDate = Calendar.getInstance();
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeformat = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat milisecondformat = new SimpleDateFormat("SSS");

        // console information printing
        System.out.println("\n" + this.date + " " + this.time
                + " | Quax Address: " + address
                + " | Data: " + data);

        if (data.contains("H")) {
            // mode = "Humidity";
            // for light and humidity array counter starts from third character
            // get first array of digits or dot, put them in "line"
            for (int i = 2; i < strarray.length; i++) {
                c = strarray[i];
                if (!(Character.isDigit(c)) && (c != '.')) {
                    break;
                }
                line.append(c);
            }

            this.date = dateformat.format(currentDate.getTime());
            this.time = timeformat.format(currentDate.getTime());
            this.milis = milisecondformat.format(currentDate.getTimeInMillis());
            this.sensor = address;

            if (TEXT_MODE) {
                WriteToText(sensor + " " + this.date + " " + this.time + " Humidity " + line.toString());
            }

            light = Double.parseDouble(line.toString());

            if (SQL_MODE) {
                if (mode.equals("Light")) {
                    WriteToDB(this.sensor, this.date, this.time, this.milis, light, "l");
                } else if (mode.equals("Humidity")) {
                    WriteToDB(this.sensor, this.date, this.time, this.milis, light, "h");
                }
            }
            System.out.println("Humidity = " + line);
            line.delete(0, line.length()); //clear string buffer
        }
        
        if (data.contains("L")) {
            for (int i = 2; i < strarray.length; i++) {
                c = strarray[i];
                if (!(Character.isDigit(c)) && (c != '.')) {
                    break;
                }
                line.append(c);
            }

            this.date = dateformat.format(currentDate.getTime());
            this.time = timeformat.format(currentDate.getTime());
            this.milis = milisecondformat.format(currentDate.getTimeInMillis());
            this.sensor = address;

            if (TEXT_MODE) {
                WriteToText(sensor + " " + this.date + " " + this.time + " Luminance " + line.toString());
            }

            light = Double.parseDouble(line.toString());

            if (SQL_MODE) {
                WriteToDB(this.sensor, this.date, this.time, this.milis, light, "l");
            }
            System.out.println("Luminance = " + light);
            line.delete(0, line.length()); //clear string buffer
        }
        if (data.contains("T") && !data.contains("H")) {

            String templine[] = data.split(" "); //T = + 245000 C 4 -> T,=,+,245000,C,4
            if (templine.length > 3) {
                line.append(templine[2]); //+_
                line.append(templine[3]); //245000
            }

            this.date = dateformat.format(currentDate.getTime());
            this.time = timeformat.format(currentDate.getTime());
            this.milis = milisecondformat.format(currentDate.getTimeInMillis());
            this.sensor = address;

            if (line.toString().equals("")) {
                System.out.println("Empty String");
            } else {
                if (TEXT_MODE) {
                    WriteToText(sensor + " " + this.date + " " + this.time + " Temperature " + line.toString());
                }
                temperature = Double.parseDouble(line.toString()) / 10000; // divide with 10000
                if (SQL_MODE) {
                    WriteToDB(this.sensor, this.date, this.time, this.milis, temperature, "t");
                }
                System.out.println("Temperature = " + temperature);
            }
            line.delete(0, line.length()); //clear string buffer           
        }

    }

    /**
     *
     * @param value
     * @return
     */
    public static byte[] intToByteArray(int value) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte) ((value >>> offset) & 0xFF);
        }
        return b;
    }

    /**
     *
     * @param input_buf
     */
    public String[] packetProcessing(byte[] input_buf) {
        // PACKET PROCESSING
        String[] str = new String[2];
        String addr = "";

        pkg_state = "S_SYN";

        int i = 0, y;
        while (i < input_buf.length) {
            if (pkg_state.equals("S_SYN")) {
                if (input_buf[i] == H_SYN) {
                    pkg_state = "S_STX";
                }
            } else if (pkg_state.equals("S_STX")) {
                if (input_buf[i] == H_STX) {
                    pkg_state = "S_DLE";
                } else {
                    pkg_state = "S_SYN";
                }
                //break;
            } else if (pkg_state.equals("S_DLE")) {
                if (input_buf[i] == H_DLE) {
                    pkg_state = "S_LENGTH";
                } else {
                    pkg_state = "S_SYN";
                }
                //break;
            } else if (pkg_state.equals("S_LENGTH")) {
                new_pkg_length = (short) input_buf[i];
                new_pkg_pointer = 0;
                pkg_state = "S_DATA";
                //break;
            } else if (pkg_state.equals("S_DATA")) {
                if (new_pkg_pointer < new_pkg_length) {
                    new_pkg[new_pkg_pointer] = input_buf[i];
                    new_pkg_pointer++;
                } else if (input_buf[i] == H_DLE) {
                    pkg_state = "S_ETX";
                } else {
                    pkg_state = "S_SYN";
                }
                //break;
            } else if (pkg_state.equals("S_ETX")) {
                if (input_buf[i] == H_ETX) {
                    pkg_state = "S_SYN";
                    pkg_type = new_pkg[0];
                    pkg_length = (short) (new_pkg_length - 14); //////////FIAT 13 or 14 for XBEE2
                    pkg_data = new byte[pkg_length];
                    for (y = 0; y < pkg_length; y++) {
                        pkg_data[y] = new_pkg[y + 14]; ///////////FIAT 13 or 14 for XBEE2
                    }
                    addr = formatByteString(new_pkg[3]);
                    addr += formatByteString(new_pkg[4]);
                    addr += formatByteString(new_pkg[5]);
                    addr += formatByteString(new_pkg[6]);
                    addr += formatByteString(new_pkg[7]);
                    addr += formatByteString(new_pkg[8]);
                    addr += formatByteString(new_pkg[9]);
                    addr += formatByteString(new_pkg[10]);

                    packetdata = new String(pkg_data);
                } else {
                    pkg_state = "S_SYN";
                }
            }
            i++;
        }
        str[0] = addr;
        str[1] = packetdata;
        return str;
        // END PACKET PROCESSING
    }

    /**
     *
     * @param b
     * @return
     */
    public String formatByteString(byte b) {
        String bytestr = "";

        bytestr = String.format("%X", b);
        if (bytestr.length() < 2) {
            bytestr = "0" + bytestr;
        }

        return bytestr;
    }

    public void test(String hostname, int port) {
        try {

            // tcp client - reads data from gateway
            Socket socket = new Socket(hostname, port);
            System.out.println("Connected to " + socket.getInetAddress() + " on port " + socket.getPort());
            System.out.println("Received data: ");

            DataInputStream dis = new DataInputStream(socket.getInputStream());

            byte[] input = new byte[512];
            byte[] inputarray = null;
            String[] packetcontent = new String[2];
            byte b;
            int j = 0;

            // data reading loop
            while (true) {
                b = dis.readByte();
                input[j] = b;

                if (b == H_ETX) {
                    // new byte array with packet length
                    inputarray = new byte[j + 2];

                    // copy packet bytes to array with fixed length
                    for (int i = 0; i < inputarray.length - 1; i++) {
                        inputarray[i] = input[i];
                    }

                    // next byte is SYN code
                    inputarray[j + 1] = dis.readByte(); // read SYN
                    j = 0;

                    // extract packet information
                    packetcontent = packetProcessing(inputarray);

                    parseData(packetcontent);
                }

                j++;
            }


        } catch (ConnectException ex) {
            System.err.println("ERROR: Cannot connect to PRISMA Gateway");

        } catch (java.net.NoRouteToHostException ex) {
            System.err.println("ERROR: Not connected to PRISMA WiFi");

        } catch (UnknownHostException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);

        }
    }
}
