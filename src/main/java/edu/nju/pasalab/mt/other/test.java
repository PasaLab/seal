package edu.nju.pasalab.mt.other;

import java.io.*;

public class test {
    public static void main(String[] args) throws IOException{

        InputStreamReader inStrReader1 = new InputStreamReader(
                new FileInputStream("data\\process\\newstest2017-enzh-src.zh.step1"), "UTF-8");
        BufferedReader br1 = new BufferedReader(inStrReader1);
        String str1 = br1.readLine();

        InputStreamReader inStrReader2 = new InputStreamReader(
                new FileInputStream("data\\process\\newsdev2017-enzh-ref.en.step1"), "UTF-8");
        BufferedReader br2 = new BufferedReader(inStrReader2);
        String str2 = br2.readLine();


        OutputStreamWriter outStreamWriter1 = new OutputStreamWriter(
                new FileOutputStream("data\\process\\newstest2017.en2zh.dev"), "UTF-8");
        BufferedWriter br3= new BufferedWriter(outStreamWriter1);

        OutputStreamWriter outStreamWriter2 = new OutputStreamWriter(
                new FileOutputStream("data\\process\\newstest2017.zh2en.dev"), "UTF-8");
        BufferedWriter br4 = new BufferedWriter(outStreamWriter2);

        while (str1 != null&& str2 != null){
            br3.append(str2 + "\n");
            br3.append("notree\n");
            br3.append(str1 + "\n");
            br3.append("========\n");

            br4.append(str1 + "\n");
            br4.append("notree\n");
            br4.append(str2 + "\n");
            br4.append("========\n");


            str1 = br1.readLine();
            str2 = br2.readLine();
        }

        if (str1 != null || str2 != null)
            System.out.println("WTF");

        br1.close();
        inStrReader1.close();

        br2.close();
        inStrReader2.close();

        br3.close();
        outStreamWriter1.close();

        br4.close();
        outStreamWriter2.close();
    }
}
