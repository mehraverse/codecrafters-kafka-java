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
    try (ServerSocket kafkaServerSocket = new ServerSocket(9092)) {
      kafkaServerSocket.setReuseAddress(true);
      try (Socket kafkaClientSocket = kafkaServerSocket.accept()) {
        byte[] clientRequest = new byte[12];
        if (kafkaClientSocket.getInputStream().read(clientRequest) != 12) {
          throw new IOException("Incomplete request received");
        }

        byte[] apiVersionBytes = { clientRequest[6], clientRequest[7] };
        byte[] correlationIdBytes = { clientRequest[8], clientRequest[9], clientRequest[10], clientRequest[11] };
        int apiVersion = ((apiVersionBytes[0] & 0xFF) << 8) | (apiVersionBytes[1] & 0xFF);

        kafkaClientSocket.getOutputStream().write(new byte[] { 0, 0, 0, 0 });
        kafkaClientSocket.getOutputStream().write(correlationIdBytes);
        kafkaClientSocket.getOutputStream().write(apiVersion < 0 || apiVersion > 4
            ? new byte[] { 0, 35 } // error_code 35
            : apiVersionBytes);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}