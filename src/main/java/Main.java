import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");

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

        // Prepare the response
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        try (DataOutputStream responseStream = new DataOutputStream(responseBuffer)) {
          responseStream.write(correlationIdBytes); // Write correlation ID
          if (apiVersion < 0 || apiVersion > 4) {
            responseStream.write(new byte[] { 0, 35 }); // error_code 35
          } else {
            responseStream.write(apiVersionBytes); // Write API version
          }
        }

        byte[] responseBody = responseBuffer.toByteArray();

        // Write the total length of the response as the first 4 bytes
        DataOutputStream outputStream = new DataOutputStream(kafkaClientSocket.getOutputStream());
        outputStream.writeInt(responseBody.length); // Total length
        outputStream.write(responseBody); // Response body
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}