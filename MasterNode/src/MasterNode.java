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
			String workerSentence3 = sendToWorker(clientSentence,"10.102.55.21");
			String workerSentence4 = sendToWorker(clientSentence,"10.102.55.11");
			String workerSentence5 = sendToWorker(clientSentence,"10.102.55.25");
			String workerSentence6 = sendToWorker(clientSentence,"10.102.55.24");
			String workerSentence7 = sendToWorker(clientSentence,"10.102.55.22");
			String workerSentence8 = sendToWorker(clientSentence,"10.102.59.25");
			String workerSentence9 = sendToWorker(clientSentence,"10.102.59.20");
			String workerSentence10 = sendToWorker(clientSentence,"10.102.59.22");
			String workerSentence11 = sendToWorker(clientSentence,"10.102.59.26");
			outToClient.writeBytes(workerSentence + "/" + workerSentence2 + "/" + workerSentence3 + "/" + workerSentence4 + "/" + workerSentence5 
					+ "/" + workerSentence6 + "/" + workerSentence7 + "/" + workerSentence8 
						+ "/" + workerSentence9 + "/" + workerSentence10 + "/" + workerSentence11+ "\n");
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
		output = output.trim();
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
