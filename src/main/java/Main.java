import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args) {
    // Log message for testing purposes
    System.err.println("Logs from your program will appear here!");

    int port = 9092;
    ExecutorService threadPool = Executors.newCachedThreadPool();
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      // Allow reuse of the address
      serverSocket.setReuseAddress(true);
      System.out.println("Server started, waiting for connections...");

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

        // Handle each client with our ClientHandler class
        threadPool.execute(new ClientHandler(clientSocket));
      }

    } catch (IOException e) {
      System.out.println("Error starting server: " + e.getMessage());
    } finally {
      threadPool.shutdown();
    }
  }
}
