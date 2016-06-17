/**
 * Author : Kim Kiogora Usage : Apache way of forking processes for High
 * Performance Applications that require high throughput
 */
package afork;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author kkiogora
 */
public class Processor {

    MySQL c;

    public Processor(MySQL c){
        this.c =c;
    }

    /**
     * Updates the record status.
     *
     * @param messageID
     * @param src
     * @param dest
     * @param msg
     */
    public void process(int messageID, String src, String dest, String msg) {
        updateRecordStatus(messageID, "success");
    }

    /**
     * The actual db update.
     * @param messageID
     * @param status
     */
    private void updateRecordStatus(int messageID, String status){
         String query = "UPDATE messages SET bucketID=0,processed=1,status=?"
                + " ,numberOfSends=(numberOfSends+1) WHERE messageID=?";

        PreparedStatement stmt;
        int rowsAffected = 0;

        try {
            Connection conn = this.c.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.setString(1, status);
            stmt.setInt(2, messageID);
            rowsAffected = stmt.executeUpdate();
            stmt.close();
            conn.close();
            System.out.println(rowsAffected+ " row(s) updated: ID "
                    +messageID+" by thread no "
                    +Thread.currentThread().getName());
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }
}
