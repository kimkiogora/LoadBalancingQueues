/**
 * Author : Kim Kiogora Usage : Apache way of forking processes for High
 * Performance Applications that require high throughput
 */
package afork;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author kkiogora
 */
public class Afork extends Thread {

    private String MESSAGE_TYPE_INFO = "INFO";
    private String MESSAGE_TYPE_DEBUG = "DEBUG";
    private String MESSAGE_TYPE_ERROR = "ERROR";
    private long SLEEP_TIME = 5000;
    private transient MySQL mysql;
    private static int batchID = 51;
    private int MAX_RECORD_LIMIT = 100;
    int db_pool_connections = 3;
    String db_params[] = {
        "localhost",
        "3306",
        "research",
        "root",
        "r00t",
        "AforkPool"
    };

    /**
     * Constructor.
     */
    public Afork() {

        //Initialize mysql connection.
        try {
            mysql = new MySQL(db_params[0], db_params[1], db_params[2],
                    db_params[3], db_params[4], db_params[5],
                    this.db_pool_connections);
        } catch (ClassNotFoundException ce) {
            log(MESSAGE_TYPE_ERROR, ce.getMessage());
        } catch (InstantiationException ie) {
            log(MESSAGE_TYPE_ERROR, ie.getMessage());
        } catch (IllegalAccessException ile) {
            log(MESSAGE_TYPE_ERROR, ile.getMessage());
        } catch (SQLException ex) {
            log(MESSAGE_TYPE_ERROR, ex.getMessage());
        }

        //Then lets get to work on the tasks allocated to us.
        this.begin();
    }

    private void begin(){
        if (mysql != null) {
            while (true) {
                ArrayList<MQ> queues = checkQueues();

                if (queues.size() <= 0) {
                    log(MESSAGE_TYPE_INFO, "No records to process");
                } else {
                    int i = 0;
                    for (MQ r : queues) {
                        String queueis = r.getQueue_name();
                        int sz = r.getQueue_size();

                        System.out.println("Size of queue "
                                + queueis + " is " + sz);

                        Runnable worker = new Worker(mysql, queueis, sz,
                                batchID);

                        if (sz > 0) {
                            Thread t = new Thread(worker);
                            t.setPriority(Thread.MIN_PRIORITY);
                            t.start();
                            batchID++;
                        }
                        i++;
                    }
                }
                queues.clear();
                log(MESSAGE_TYPE_INFO, "Current batchID is => " + batchID);
                waitFor(SLEEP_TIME);
            }
        }
    }

    /**
     * Worker Thread. Processes the tasks in each unique queue.
     */
    class Worker implements Runnable {

        transient String queueName;
        transient MySQL mysql;
        transient int batchID;
        transient int current_queue_size;
        int max_worker_threads = 3;
        int dynamic_limit = MAX_RECORD_LIMIT;
        long sleep_time = 3000;

        /**
         * Constructor.
         *
         * @param mysql
         * @param queue
         * @param curr_qsize
         * @param batchID
         */
        public Worker(MySQL mysql, String queue, int curr_qsize, int batchID) {
            this.queueName = queue;
            this.current_queue_size = curr_qsize;
            this.mysql = mysql;
            this.batchID = batchID;

            if (this.current_queue_size < MAX_RECORD_LIMIT)
                dynamic_limit = this.current_queue_size;

            System.out.println(queueName + " @ size "+ curr_qsize + ", "
                    + "default limit @ "+dynamic_limit);
        }

        @Override
        public void run() {
            while (true) {
                int records_affected = markBatch(this.batchID,
                        this.queueName, dynamic_limit);
                if (records_affected > 0) {
                    System.out.println(
                            "Found " + records_affected + " record(s) "
                            + "marked for processing, queue_name: "
                            + queueName);

                    ArrayList<SMS> tasks = processBatch(this.batchID,
                            this.queueName);


                    log(MESSAGE_TYPE_DEBUG, "Process task >>>");

                    this.current_queue_size = tasks.size();

                    //Process Faster
                    //Attempt to speed up the processing by forking out at least
                    //2 threads until thread shutsdown as we have no mechanism
                    //for rechecking the queue size or do we
                    int N = (this.current_queue_size / MAX_RECORD_LIMIT);
                    if (N > 2 || N==1 )N=max_worker_threads;

                    log(MESSAGE_TYPE_DEBUG, queueName+" current queue size is "
                            +current_queue_size);

                    if (this.current_queue_size >= MAX_RECORD_LIMIT) {

                        log(MESSAGE_TYPE_DEBUG, queueName + " queue has been "
                                + "split for faster processing , now worker has"
                                + " "+N+" threads workjng on queue size of "
                                + this.current_queue_size);

                        ExecutorService executor
                                = Executors.newFixedThreadPool(N);
                        for (final SMS t : tasks) {
                            executor.execute(new Runnable() {
                                @Override
                                public void run() {
                                    Processor p = new Processor(mysql);
                                    p.process(t.getMessageID(),
                                            t.getSourceAddr(),t.getDestaddr(),
                                            t.getMessage());
                                }
                            });
                        }
                        executor.shutdown();
                        //Finished all threads.
                    } else {
                         log(MESSAGE_TYPE_DEBUG, queueName + " Process queue "
                                 + "normally size is at "
                                 + this.current_queue_size);
                        for (SMS t : tasks) {
                            Processor p = new Processor(mysql);
                            p.process(t.getMessageID(), t.getSourceAddr(),
                                    t.getDestaddr(), t.getMessage());
                        }
                    }
                    tasks.clear();
                }

                //Kill if nothing in db
                if (records_affected <= 0) {
                    log(MESSAGE_TYPE_DEBUG, queueName.concat(" shutting down as"
                            + " there are currently no records to process >>>")
                            );
                    break;
                }

                waitFor(sleep_time);
            }
        }
    }

    /**
     * Check the list of queues.
     *
     * @return
     */
    private ArrayList<MQ> checkQueues() {
        ArrayList<MQ> queues = new ArrayList<MQ>();
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        String query = "select count(*) as hits, sourceaddr from messages where"
                + " status='pending' and batchID=0 group by sourceaddr";
        try {
            conn = mysql.getConnection();
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            while (rs.next()) {
                MQ q = new MQ();
                q.setQueue_name(rs.getString("sourceaddr"));
                q.setQueue_size(rs.getInt("hits"));
                queues.add(q);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException sqe) {
            log(MESSAGE_TYPE_ERROR, "checkQueues".concat(sqe.getMessage()));
        }

        return queues;
    }

    /**
     * Fetch all the records that have been marked for processing.
     *
     * @param batchID
     * @return
     */
    private ArrayList<SMS> processBatch(int batchID,
            String queuename) {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        String query = "SELECT messageID, MSISDN, message from outbound where"
                + " batchID=? and sourceaddr=?";
        ArrayList<SMS> work_list = new ArrayList<SMS>();
        try {
            conn = mysql.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.setInt(1, batchID);
            stmt.setString(2, queuename);
            rs = stmt.executeQuery();
            while (rs.next()) {
                SMS sms = new SMS();
                sms.setOutboundID(rs.getInt("messageID"));
                sms.setMessage(rs.getString("message"));
                sms.setDestaddr(rs.getString("MSISDN"));
                sms.setSourceAddr("Cellulant");
                work_list.add(sms);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException sqe) {
            log(MESSAGE_TYPE_ERROR, "markBatch".concat(sqe.getMessage()));
        }

        return work_list;
    }

    /**
     * Mark a record for processing by assigning a unique ID to a batch of
     * records
     *
     * @param batchID
     * @param limit
     * @return integer
     */
    private int markBatch(int batchID, String queuename, int limit) {
        String query = "UPDATE messages SET batchID=" + batchID
                + " WHERE batchID=0 AND processed=0 AND status='pending'"
                + " AND numberOfSends=0 AND sourceaddr=? LIMIT ?";

        System.out.println("allocate queue { "+queuename+" } with limit "+limit);

        Connection conn;
        PreparedStatement stmt;
        int rowsAffected = 0;

        try {
            conn = mysql.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.setString(1, queuename);
            stmt.setInt(2, limit);
            rowsAffected = stmt.executeUpdate();
            stmt.close();
            conn.close();
            System.out.println("Queue= " + queuename + " GIVEN batchID=" + batchID);
        } catch (SQLException sqe) {
            log(MESSAGE_TYPE_ERROR, "markBatch".concat(sqe.getMessage()));
        }
        return rowsAffected;
    }

    /**
     * Wait for a bit.
     */
    private void waitFor(long wait) {
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Logs a message.
     *
     * @param messageType
     * @param msg
     */
    private void log(String messageType, String msg) {
        System.out.println(messageType.concat("| ".concat(msg)));
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Afork main = new Afork();
        main.start();
    }
}
