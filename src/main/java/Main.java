import java.io.DataInputStream;
import java.io.DataOutputStream;
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
    DataInputStream input;
    DataOutputStream output = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      input = new DataInputStream(clientSocket.getInputStream());
      output = new DataOutputStream(clientSocket.getOutputStream());

      int message_size = input.readInt();
      short request_api_key = input.readShort();
      short request_api_version = input.readShort();
      int correlation_id = input.readInt();

      output.writeInt(0);
      output.writeInt(correlation_id);
      output.writeShort(35);
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
