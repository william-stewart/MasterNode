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
	static String workerSentence;
	
	public static void receiveAndReturn() throws Exception{
		ServerSocket welcomeSocket = new ServerSocket(6789);
		
		while(true){
			Socket connectionSocket = welcomeSocket.accept();
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
			DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
			clientSentence = "";
			workerSentence = "";
			clientSentence = inFromClient.readLine();
			workerSentence = sendToWorker(clientSentence,"10.102.55.23");
			String workerSentence2 = sendToWorker(clientSentence,"10.102.55.20");
			workerSentence.replaceAll("\\s+", "");
			workerSentence2.replaceAll("\\s+", "");
			outToClient.writeBytes(workerSentence + "/" + workerSentence2 + "\n");
		}
	}
	
	public static String sendToWorker(String input,String IPAddr) throws Exception{
		DatagramSocket clientSocket = new DatagramSocket();
		InetAddress IPAddress = InetAddress.getByName(IPAddr);
		byte[] sendData = new byte[1024];
		byte[] receiveData = new byte[1024];
		sendData = input.getBytes();
		DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,IPAddress,9875);
		clientSocket.send(sendPacket);
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		clientSocket.receive(receivePacket);
		String output = new String(receivePacket.getData());
		clientSocket.close();
		//workerSentence = output + "\n";
		output = output + "\n";
		System.out.println("from worker: " + output);
		return output;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Pulled from mac
		//And from pc
		try{
			receiveAndReturn();
		}
		catch(Exception e){
		}
		
	}
}
