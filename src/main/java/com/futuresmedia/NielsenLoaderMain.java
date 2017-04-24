package com.futuresmedia;

/**
 * Created by sjoseph on 4/19/2017.
 */

//for opencsv
import java.io.FileReader;
import java.sql.ResultSet;
import java.util.Arrays;

import au.com.bytecode.opencsv.CSVReader;


public class NielsenLoaderMain {


    public static void main(String args[]) throws Exception {


        //for execution time calculation.
        final long startTime = System.currentTimeMillis();

        final String dir = System.getProperty("user.dir");
        System.out.println("current dir = " + dir);


        //For the prepared statement

        FMDBConnector dbConnector = new FMDBConnector("localhost","sjoseph","zabar","rfi_development");
        String sqlstatement = "insert into nielsen_dailyl3(custom_range,date,data_stream,daypart,start_time,end_time,station," +
                "p1849_ratings,p1849_impressions,p1849_share_percent,p1849_hut_percent,p1849_hut_projected," +
                "m1849_ratings,m1849_impressions,m1849_share_percent,m1849_hut_percent,m1849_hut_projected," +
                "f1849_ratings,f1849_impressions,f1849_share_percent,f1849_hut_percent,f1849_hut_projected," +
                "p2554_ratings,p2554_impressions,p2554_share_percent,p2554_hut_percent,p2554_hut_projected," +
                "m2554_ratings,m2554_impressions,m2554_share_percent,m2554_hut_percent,m2554_hut_projected," +
                "f2554_ratings,f2554_impressions,f2554_share_percent,f2554_hut_percent,f2554_hut_projected," +
                "hhld_ratings,hhld_impressions,hhld_share_percent,hhld_hut_percent,hhld_hut_projected," +
                "p18plus_ratings,p18plus_impressions,p18plus_share_percent,p18plus_hut_percent,p18plus_hut_projected," +
                "m18plus_ratings,m18plus_impressions,m18plus_share_percent,m18plus_hut_percent,m18plus_hut_projected," +
                "f18plus_ratings,f18plus_impressions,f18plus_share_percent,f18plus_hut_percent,f18plus_hut_projected," +
                "p1834_ratings,p1834_impressions,p1834_share_percent,p1834_hut_percent,p1834_hut_projected," +
                "p3554_ratings,p3554_impressions,p3554_share_percent,p3554_hut_percent,p3554_hut_projected," +
                "p35plus_ratings,p35plus_impressions,p35plus_share_percent,p35plus_hut_percent,p35plus_hut_projected," +
                "p55plus_ratings,p55plus_impressions,p55plus_share_percent,p55plus_hut_percent,p55plus_hut_projected," +
                "p65plus_ratings,p65plus_impressions,p65plus_share_percent,p65plus_hut_percent,p65plus_hut_projected," +
                "m1834_ratings,m1834_impressions,m1834_share_percent,m1834_hut_percent,m1834_hut_projected," +
                "m35plus_ratings,m35plus_impressions,m35plus_share_percent,m35plus_hut_percent,m35plus_hut_projected," +
                "f35plus_ratings,f35plus_impressions,f35plus_share_percent,f35plus_hut_percent,f35plus_hut_projected," +
                "m3554_ratings,m3554_impressions,m3554_share_percent,m3554_hut_percent,m3554_hut_projected," +
                "f3554_ratings,f3554_impressions,f3554_share_percent,f3554_hut_percent,f3554_hut_projected," +
                "m55plus_ratings,m55plus_impressions,m55plus_share_percent,m55plus_hut_percent,m55plus_hut_projected," +
                "f55plus_ratings,f55plus_impressions,f55plus_share_percent,f55plus_hut_percent,f55plus_hut_projected," +
                "m65plus_ratings,m65plus_impressions,m65plus_share_percent,m65plus_hut_percent,m65plus_hut_projected," +
                "f65plus_ratings,f65plus_impressions,f65plus_share_percent,f65plus_hut_percent,f65plus_hut_projected," +
                "f1834_ratings,f1834_impressions,f1834_share_percent,f1834_hut_percent,f1834_hut_projected," +
                "p2plus_ratings,p2plus_impressions,p2plus_share_percent,p2plus_hut_percent,p2plus_hut_projected," +
                "p211_ratings,p211_impressions,p211_share_percent,p211_hut_percent,p211_hut_projected," +
                "p611_ratings,p611_impressions,p611_share_percent,p611_hut_percent,p611_hut_projected," +
                "p1824_ratings,p1824_impressions,p1824_share_percent,p1824_hut_percent,p1824_hut_projected," +
                "p1854_ratings,p1854_impressions,p1854_share_percent,p1854_hut_percent,p1854_hut_projected," +
                "p25plus_ratings,p25plus_impressions,p25plus_share_percent,p25plus_hut_percent,p25plus_hut_projected," +
                "p2534_ratings,p2534_impressions,p2534_share_percent,p2534_hut_percent,p2534_hut_projected," +
                "p2549_ratings,p2549_impressions,p2549_share_percent,p2549_hut_percent,p2549_hut_projected," +
                "p2564_ratings,p2564_impressions,p2564_share_percent,p2564_hut_percent,p2564_hut_projected," +
                "p3549_ratings,p3549_impressions,p3549_share_percent,p3549_hut_percent,p3549_hut_projected," +
                "p3564_ratings,p3564_impressions,p3564_share_percent,p3564_hut_percent,p3564_hut_projected," +
                "p40plus_ratings,p40plus_impressions,p40plus_share_percent,p40plus_hut_percent,p40plus_hut_projected," +
                "p50plus_ratings,p50plus_impressions,p50plus_share_percent,p50plus_hut_percent,p50plus_hut_projected," +
                "f1824_ratings,f1824_impressions,f1824_share_percent,f1824_hut_percent,f1824_hut_projected," +
                "f1854_ratings,f1854_impressions,f1854_share_percent,f1854_hut_percent,f1854_hut_projected," +
                "f25plus_ratings,f25plus_impressions,f25plus_share_percent,f25plus_hut_percent,f25plus_hut_projected," +
                "f2534_ratings,f2534_impressions,f2534_share_percent,f2534_hut_percent,f2534_hut_projected," +
                "f2549_ratings,f2549_impressions,f2549_share_percent,f2549_hut_percent,f2549_hut_projected," +
                "f3549_ratings,f3549_impressions,f3549_share_percent,f3549_hut_percent,f3549_hut_projected," +
                "f3564_ratings,f3564_impressions,f3564_share_percent,f3564_hut_percent,f3564_hut_projected," +
                "f40plus_ratings,f40plus_impressions,f40plus_share_percent,f40plus_hut_percent,f40plus_hut_projected," +
                "f50plus_ratings,f50plus_impressions,f50plus_share_percent,f50plus_hut_percent,f50plus_hut_projected," +
                "m1824_ratings,m1824_impressions,m1824_share_percent,m1824_hut_percent,m1824_hut_projected," +
                "m1854_ratings,m1854_impressions,m1854_share_percent,m1854_hut_percent,m1854_hut_projected," +
                "m25plus_ratings,m25plus_impressions,m25plus_share_percent,m25plus_hut_percent,m25plus_hut_projected," +
                "m2534_ratings,m2534_impressions,m2534_share_percent,m2534_hut_percent,m2534_hut_projected," +
                "m2549_ratings,m2549_impressions,m2549_share_percent,m2549_hut_percent,m2549_hut_projected," +
                "m3549_ratings,m3549_impressions,m3549_share_percent,m3549_hut_percent,m3549_hut_projected," +
                "m3564_ratings,m3564_impressions,m3564_share_percent,m3564_hut_percent,m3564_hut_projected," +
                "m40plus_ratings,m40plus_impressions,m40plus_share_percent,m40plus_hut_percent,m40plus_hut_projected," +
                "m50plus_ratings,m50plus_impressions,m50plus_share_percent,m50plus_hut_percent,m50plus_hut_projected) VALUES " +
                "(?,?::Date,?,?,?::Time,?::Time,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

         //debug
        //System.out.println(sqlstatement);

        //change this
        String str_filename = dir + "\\FMLivePlus3 04-18-2017 to 04-18-2017.csv";
        CSVReader reader = new CSVReader(new FileReader(str_filename), ',' , '"' , 6);

        //Read CSV line by line and use the string array as you want
        String[] nextLine;
        String str_holder;
        String main_delim = "[,]";
        String time_delim = "[-]";
        String str_holder_forTypeConversion;

        Object[]  obj_split_array;
        Object[] obj_holder_array = new Object[287];

        String[]  str_TimeSplit_array;

        String rsholder;

        while ((nextLine = reader.readNext()) != null) {
            if (nextLine != null) {
                //Verifying the read data here
                str_holder = Arrays.toString(nextLine);

                //debug
                //System.out.println(str_holder);

                //splitting the line
                obj_split_array = str_holder.split(main_delim);

                int array_length = obj_split_array.length;
                //debug
                //System.out.println("length of array = " + array_length);
                //System.out.println("position First = " + obj_split_array[0]);
                //System.out.println("position Last = " + obj_split_array[array_length-1]);

                //String firstposition = obj_split_array[0].toString();
                //String lastposition =  obj_split_array[array_length-1].toString();

                //trimming out the square brackets
                obj_split_array[0] = obj_split_array[0].toString().replace("[","");
                obj_split_array[array_length-1] = obj_split_array[array_length-1].toString().replace("]","");

                //splitting the time field
                str_TimeSplit_array =  obj_split_array[2].toString().split(time_delim);

                //debug
                //System.out.println("drop " + obj_split_array[0] + " drop1 " + obj_split_array[1] + str_TimeSplit_array[0] + str_TimeSplit_array[1] + obj_split_array[2] + obj_split_array[3]);

                //debuggingg
                //System.out.println("debugging ----------------------------------");
                //System.out.println("end debugging ----------------------------------");


                obj_holder_array[0] = 0; //custom range
                obj_holder_array[1] = obj_split_array[0];  //date
                obj_holder_array[2] = 0;  //Date
                obj_holder_array[3] = obj_split_array[1]; //dayparts
                obj_holder_array[4] = str_TimeSplit_array[0]; //Start Time
                obj_holder_array[5] = str_TimeSplit_array[1]; //End Time
                obj_holder_array[6] = obj_split_array[3]; // Viewing Source

                //debug
                //rsholder = "0" +  obj_split_array[0] +  "0" +  obj_split_array[1] +  str_TimeSplit_array[0] + str_TimeSplit_array[1] +  obj_split_array[3];
                //System.out.println("rsholder before ------- "  + rsholder);





                //The Impressions US AA Proj (000 s) and US HUT/PUT Proj (000 s) Columns in the Excel file are both integers
                //and have been carried as such into the Database. The section below just converts these two Projection fields for every
                //demo into an Integer type.

                int counterplusone = 1;
                int k;


                for (int j = 4; j <  obj_split_array.length; j++) {

                        //we use k because the second array has things that correspond to the db (and not the execel file) like start time and end time
                        k=j+3;

                        if (j % 5==0) {
                            obj_holder_array[k] = (int)Double.parseDouble(obj_split_array[j].toString().trim());
                        }
                        else if (counterplusone == 5) {
                            obj_holder_array[k] =  (int)Double.parseDouble(obj_split_array[j].toString().trim());
                            counterplusone = 0;
                        }
                        else {
                            obj_holder_array[k] =  Double.parseDouble(obj_split_array[j].toString().trim());
                            }

                        //debug
                        //System.out.println(j + " The value is = " + obj_split_array[j].toString());
                        //System.out.println("Old array = " + obj_split_array[j].toString() + " | New array = " + obj_holder_array[j+3].toString());
                        //System.out.println("Old array = " + j + "  || New array = " +  k );
                    counterplusone++;
                }

                //end section.

                //inserting the prepared statement into the DB
                ResultSet rs = dbConnector.insertOrUpdateQuery(sqlstatement,obj_holder_array);


            }
        }
        //outside While before the query ends...

        // Committing the prepared statement into the DB
        dbConnector.commit();


        //for execution time calculation.
        final long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) );

    } //PSVM

}//end NielsenLoaderMain
