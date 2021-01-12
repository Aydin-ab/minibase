package tests;

import java.lang.*;
import java.io.*;

/** 
   * @param filename
   * @param path
   * 
   * @exception FileNotFoundException the text file is not found
   */

public class FileQuery {
    String[] projectStrings;
    String[] relationsStrings;
    String[] condExprStrings_1;
    String[] condExprStrings_2;

    //constructor
    public FileQuery (String filename, String path, int conds) throws FileNotFoundException {
        File file = new File(path + "/" + filename);
        String pth =  new String(path);

        if (file.exists()) {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            try {
                String atr = br.readLine();
                projectStrings = atr.trim().split("\\s+");

                String rd = br.readLine();
                relationsStrings = rd.trim().split("\\s+");

                String cexp_1 = br.readLine();
                condExprStrings_1 = cexp_1.trim().split("\\s+");
                
                // other conditions for two inequality predicates
                if (conds > 1) {
                	// Skip the and line
                	String and = br.readLine();
                    String cexp_2 = br.readLine();
                    condExprStrings_2 = cexp_2.trim().split("\\s+");
                	
                }
                
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                    if (fr != null)
                        fr.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else{
                System.out.println(path + filename + " does not exist");
                //System.exit(1);
            }
        }

    public String[] getAttributeStrings() {
        return projectStrings;
    }

    public String[] getRelationsStrings() {
        return relationsStrings;
    }
    public String[] getConStrings_1() {
        return condExprStrings_1;
    }
    public String[] getConStrings_2() {
        return condExprStrings_2;
    }
}
