import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      byte[] request = new byte[12];
      int bytesRead = clientSocket.getInputStream().read(request);
      if (bytesRead != 12) {
        throw new IOException("Incomplete request received");
      }
      byte[] correlation_id = new byte[4];
      System.arraycopy(request, 8, correlation_id, 0, 4);
      clientSocket.getOutputStream()
          .write(new byte[] { 0, 0, 0, 0, correlation_id[0], correlation_id[1], correlation_id[2], correlation_id[3] });
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
