import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataImport {
    public static void main(String[] args) throws IOException {
        Cluster cluster = Cluster.builder().addContactPoints("ec2-54-172-128-31.compute-1.amazonaws.com").withPort(9042).
                withCredentials("cassandra", "cassandra").build();
        Session session = cluster.connect();

        System.out.println("connect ok, start insert data");

        FileInputStream inputStream = new FileInputStream(args[0]);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        int i=0;
        String str = null;
        while((str = bufferedReader.readLine()) != null)
        {
            String []data = str.split(",");
            String cql = String.format("INSERT INTO capstone.q32 (date, src, dest, carrier, flight, deptime, delay) VALUES ('%s','%s','%s','%s','%s',%s,'%s')",
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6]);
            session.execute(cql);
            i++;
            if (i % 10000 == 0) {
                System.out.println(i);
            }
        }

        System.out.println("insert complete");
        inputStream.close();
        bufferedReader.close();
    }
}
