import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    DataInputStream in = null;
    DataOutputStream out = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);
      clientSocket = serverSocket.accept();
      // read all bytes from the socket
      in = new DataInputStream(clientSocket.getInputStream());
      out = new DataOutputStream(clientSocket.getOutputStream());
      byte[] messageSize = in.readNBytes(4);
      byte[] apiKey = in.readNBytes(2);
      byte[] apiVersion = in.readNBytes(2);
      byte[] correlationID = in.readNBytes(4);
      // byte[] requestBody = in.readNBytes(in.available());

      ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
      // Reserve space for message length
      responseBuffer.putInt(0); // placeholder

      // Correlation ID
      responseBuffer.put(correlationID);

      // Error code
      short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
      short errorCode = (apiVersionValue < 0 || apiVersionValue > 4) ? (short) 35 : (short) 0;
      responseBuffer.putShort(errorCode);

      responseBuffer.put((byte) 2); // Compact array length = 1 element + 1
      responseBuffer.putShort((short) 18); // API Key
      responseBuffer.putShort((short) 0); // Min Version
      responseBuffer.putShort((short) 4); // Max Version

      // throttle_time_ms (INT32)
      responseBuffer.putInt(0);

      // Tagged fields
      responseBuffer.putShort((short) 0); // No tagged fields

      // Update message length
      int messageLength = responseBuffer.position() - 4;
      responseBuffer.putInt(0, messageLength);

      // Write the response
      out.write(responseBuffer.array(), 0, responseBuffer.position());
      out.flush();

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}