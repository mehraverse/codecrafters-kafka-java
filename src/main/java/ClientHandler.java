import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {

            while (!clientSocket.isClosed()) {
                // Read request message size and other data
                byte[] messageSize = new byte[4];
                in.readFully(messageSize);

                // Process other parts of the request
                byte[] apiKey = new byte[2];
                in.readFully(apiKey);
                byte[] apiVersion = new byte[2];
                in.readFully(apiVersion);
                byte[] correlationID = new byte[4];
                in.readFully(correlationID);
                in.skip(in.available());

                // Handle the request and send a response
                ByteBuffer responseBuffer = createResponseBuffer(apiVersion, correlationID);

                out.write(responseBuffer.array(), 0, responseBuffer.position());
                out.flush();
                System.out.println("Response sent to client");
            }

        } catch (IOException e) {
            System.out.println("Error while handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private ByteBuffer createResponseBuffer(byte[] apiVersion, byte[] correlationID) {
        ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
        responseBuffer.putInt(0); // Placeholder for message length
        responseBuffer.put(correlationID);
        short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
        short errorCode = (apiVersionValue < 0 || apiVersionValue > 4) ? (short) 35 : (short) 0;
        responseBuffer.putShort(errorCode);

        responseBuffer.put((byte) 3);
        // First API
        responseBuffer.putShort((short) 18); // API Versions 18
        responseBuffer.putShort((short) 0); // Min version
        responseBuffer.putShort((short) 4); // Max version
        responseBuffer.put((byte) 0); // Tagged fields for this API (compact encoded 0)

        // Second API
        responseBuffer.putShort((short) 75); // DescribeTopicPartitions 75
        responseBuffer.putShort((short) 0); // Min version
        responseBuffer.putShort((short) 0); // Max version
        responseBuffer.put((byte) 0); // Tagged fields for this API (compact encoded 0)

        responseBuffer.putInt(0); // Throttle time
        responseBuffer.put((byte) 0); // No tagged fields
        int messageLength = responseBuffer.position() - 4;
        responseBuffer.putInt(0, messageLength);
        return responseBuffer;
    }

}
