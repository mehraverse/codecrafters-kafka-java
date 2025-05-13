import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

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
                byte[] clientIDLength = new byte[2];
                in.readFully(clientIDLength);
                byte[] clientID = new byte[ByteBuffer.wrap(clientIDLength).getShort()];
                in.readFully(clientID);

                byte[] topicsArrayLength = null;
                byte[] topicNameLength = null;
                byte[] topicName = null;

                System.out.println("Client ID: " + new String(clientID));
                if (ByteBuffer.wrap(apiKey).getShort() == 75) {
                    System.out.println("Received DescribeTopicPartitions request");
                    byte[] tagBufferLength = new byte[1];
                    in.readFully(tagBufferLength);
                    System.out.println("Tag Buffer Length: " + ByteBuffer.wrap(tagBufferLength).get());

                    topicsArrayLength = new byte[1];
                    in.readFully(topicsArrayLength);
                    System.out.println("Topics Array Length: " + ByteBuffer.wrap(topicsArrayLength).get());

                    topicNameLength = new byte[1];
                    in.readFully(topicNameLength);
                    System.out.println("Topic Name Length: " + ByteBuffer.wrap(topicNameLength).get());

                    topicName = new byte[ByteBuffer.wrap(topicNameLength).get()];
                    in.readFully(topicName);
                    System.out.println("Topic Name: " + new String(topicName));

                    byte[] tagBufferLength2 = new byte[1];
                    in.readFully(tagBufferLength2);
                    System.out.println("Tag Buffer Length: " + ByteBuffer.wrap(tagBufferLength2).get());

                    byte[] responsePartitionLimit = new byte[4];
                    in.readFully(responsePartitionLimit);
                    System.out.println("Response Partition Limit: " + ByteBuffer.wrap(responsePartitionLimit).getInt());

                    byte[] cursor = new byte[1];
                    in.readFully(cursor);
                    System.out.println("Cursor: " + ByteBuffer.wrap(cursor).get());
                }

                System.out.println("Remaining bytes in input stream: " + in.available());
                in.skip(in.available());

                // Handle the request and send a response
                ByteBuffer responseBuffer = createResponseBuffer(apiKey, apiVersion, correlationID, topicsArrayLength,
                        topicNameLength, topicName);

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

    private ByteBuffer createResponseBuffer(byte[] apiKey, byte[] apiVersion, byte[] correlationID,
            byte[] topicsArrayLength, byte[] topicNameLength, byte[] topicName) {
        System.out.println("Creating response buffer");
        ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
        responseBuffer.putInt(0); // Placeholder for message length
        responseBuffer.put(correlationID);
        // If API Key == 0x4b (75) (DescribeTopicPartitions)
        if (ByteBuffer.wrap(apiKey).getShort() == 75) {
            System.out.println("Received DescribeTopicPartitions request");
            responseBuffer.put((byte) 0); // Tag Buffer
            responseBuffer.putInt(0); // Throttle time
            responseBuffer.put(topicsArrayLength);// Topic array length
            responseBuffer.putShort((short) 3); // Error code
            responseBuffer.put(topicNameLength); // Topic name length
            responseBuffer.put(topicName); // Topic name
            responseBuffer.put(new byte[16]); // 16-byte null ID
            responseBuffer.put((byte) 0); // IsInternal == 0
            responseBuffer.putInt(0x00000DF8); // TopicAuthorizedOperations
            responseBuffer.put((byte) 0); // compact-encoded empty TAG_BUFFER

            responseBuffer.put((byte) 0xff); // Cursor
            responseBuffer.put((byte) 0); // Tag Buffer
        }

        else {
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
        }
        int messageLength = responseBuffer.position() - 4;
        System.out.println("Message length: " + messageLength);
        responseBuffer.putInt(0, messageLength);
        return responseBuffer;
    }

}
