/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package message.preprocess;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author ivana
 */
public class MessagePreProcess {

    /**
     * @param args the command line arguments
     */
    static JSONParser parser = new JSONParser();

    public static String processData(int code, String input) {
        String retval = "";
        //0: formalizer, 1: stemmer
        try {
            URL url;
            if(code == 0){
                url = new URL("http://localhost:9000/formalizer");
            } else {
                url = new URL("http://localhost:9000/stemmer");
            }
            
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            JSONObject obj1 = new JSONObject();
            obj1.put("string", input);

            StringWriter out = new StringWriter();
            obj1.writeJSONString(out);

            String jsonText = out.toString();

            OutputStream os = conn.getOutputStream();
            os.write(jsonText.getBytes());
            os.flush();

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            while ((output = br.readLine()) != null) {
                Object obj = parser.parse(output);
                JSONObject json = (JSONObject) obj;
                retval += json.get("data");
            }

            conn.disconnect();

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        } catch (ParseException ex) {
            Logger.getLogger(MessagePreProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
        return retval;
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException, ParseException {
        //read csv

        PrintWriter writer = new PrintWriter("/Users/ivana/Documents/BI-Metabase/Data-30Aug2017/Amazara-Preprocessed.csv", "UTF-8");
        String csvFile = "/Users/ivana/Documents/BI-Metabase/Data-30Aug2017/Amazara-Messages.csv";
        String line = "";
        String csvSplitBy = ",";
        int counter = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            line = br.readLine();
            String[] header = line.split(csvSplitBy);
            for (int i = 0; i < header.length; i++) {
                System.out.println(header[i]);
            }
            System.out.println(header.length);
            writer.println("source,message,conversation_id,channel");
            while ((line = br.readLine()) != null) {
                String[] messages = line.split(csvSplitBy);
                if (messages.length == 13) {
                    if (messages[12].equals("text")) {
                        counter++;
                        if(counter%500==0) {
                            System.out.println("counter: " + counter);
                        }
                        //formalizer
                        String formalized = processData(0, messages[2]);
                        //stemmer
                        String stemmed = processData(1, formalized);
                        
                        if(messages[11].equals("visitor")){
                            writer.println(messages[11] + ',' + stemmed + ',' + messages[3] + ',' + messages[1]);   
                        } else {
                            writer.println("agent," + stemmed + ',' + messages[3] + ',' + messages[1]);   
                        }
                        
                    }
                }

            }

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
