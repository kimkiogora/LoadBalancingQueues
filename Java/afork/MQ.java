/**
 * Author : Kim Kiogora Usage : Apache way of forking processes for High
 * Performance Applications that require high throughput
 */
package afork;

/**
 *
 * @author kkiogora
 */
public class MQ {
    String queue_name;
    int queue_size;

    public String getQueue_name() {
        return queue_name;
    }

    public void setQueue_name(String queue_name) {
        this.queue_name = queue_name;
    }

    public int getQueue_size() {
        return queue_size;
    }

    public void setQueue_size(int queue_size) {
        this.queue_size = queue_size;
    }

    @Override
    public String toString() {
        return "MQ{" + "queue_name=" + queue_name + ", queue_size=" + queue_size + '}';
    }
}
