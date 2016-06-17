/**
 * Author : Kim Kiogora Usage : Apache way of forking processes for High
 * Performance Applications that require high throughput
 */
package afork;

/**
 *
 * @author kkiogora
 */
public class SMS {

    private int messageID;
    private String sourceAddr;
    private String message;
    private String destaddr;

    public String getDestaddr() {
        return destaddr;
    }

    public void setDestaddr(String destaddr) {
        this.destaddr = destaddr;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }

    public String getSourceAddr() {
        return sourceAddr;
    }

    public void setSourceAddr(String sourceAddr) {
        this.sourceAddr = sourceAddr;
    }

    @Override
    public String toString() {
        return "SMS{" + "messageID=" + messageID + ", sourceAddr="
                + sourceAddr + ", message=" + message + ", destaddr="
                + destaddr + '}';
    }


}
