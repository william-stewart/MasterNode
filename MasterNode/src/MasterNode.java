import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class MasterNode {
	static String clientSentence;
	static String capitalizedSentence;
	
	public static void receiveAndReturn() throws Exception{
		ServerSocket welcomeSocket = new ServerSocket(6789);
		
		while(true){
			Socket connectionSocket = welcomeSocket.accept();
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
			DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
			clientSentence = inFromClient.readLine();
			String workerSentence = sendToWorker(clientSentence);
			capitalizedSentence = clientSentence.toUpperCase() + '\n';
			outToClient.writeBytes(workerSentence);
		}
	}
	
	public static String sendToWorker(String input) throws Exception{
		DatagramSocket clientSocket = new DatagramSocket();
		InetAddress IPAddress = InetAddress.getByName("10.102.55.23");
		byte[] sendData = new byte[1024];
		byte[] receiveData = new byte[1024];
		sendData = input.getBytes();
		DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,IPAddress,9876);
		clientSocket.send(sendPacket);
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		clientSocket.receive(receivePacket);
		String output = new String(receivePacket.getData());
		clientSocket.close();
		return output;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			receiveAndReturn();
		}
		catch(Exception e){
		}
		
	}
}
