import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;


public class MasterNodeThreaded {
	static String clientSentence = "-";
	static String workerSentence;
	static String[] returnFromWorkers = new String[31];
	
	/*
	 * Sends an insignificant packet to all of the nodes to maintain a fresh connection.
	 * Is run in a separate thread that fires at designated interval in main method.
	 */
	static class RefreshConnections extends TimerTask{
		//The message that will be send
		static String blankMessage = "*";
		
		//Overwrites the run method to send the message to each node
		public void run(){
			try{
				MasterNodeThreaded.sendToWorker(blankMessage,"pi10.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi11.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi12.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi13.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi14.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi15.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi16.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi17.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi18.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi19.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi20.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi21.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi22.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi23.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi24.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi25.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi26.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi27.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi28.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi29.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi30.fu.campus");
				//MasterNodeThreaded.sendToWorker(blankMessage,"pi31.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi32.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi33.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi34.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi35.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi36.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi37.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi38.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi39.fu.campus");
				MasterNodeThreaded.sendToWorker(blankMessage,"pi40.fu.campus");
				
			}
			catch(Exception e){}
		}
	}
	
	/*
	 * Allows for a different thread for communication with each node.
	 */
	public static class SendToAllWorkers implements Runnable{
		
		static String IPAddr;
		static int piNum;
		
		public SendToAllWorkers(String clientSentenceIn, String IPAddrIn, int piNumIn){
			ensureCorrectVars(clientSentenceIn,IPAddrIn,piNumIn);
			
		}
		
		public synchronized void ensureCorrectVars(String clientSentenceIn, String IPAddrIn, int piNumIn){
			piNum = piNumIn-9;
			IPAddr = IPAddrIn;
			clientSentence = clientSentenceIn;
			try{
				returnFromWorkers[piNum-1] = sendToWorker(clientSentence,IPAddr);
			}
			catch(Exception e){
				//System.out.println("Could not contact PI:" + piNum);
			}
		}
		
		public void run(){
			//System.out.println("Started thread for pi" + piNum);
		}
	}

	/*
	 * Creates TCP Server to receive message from client and returns the data from the worker nodes
	 * @param: 
	 * @output:
	 */
	public static void receiveAndReturn() throws Exception{
		ServerSocket welcomeSocket = new ServerSocket(6789);
		
		while(true){
			//Get data from client
			Socket connectionSocket = welcomeSocket.accept();
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
			DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
			clientSentence = "";
			workerSentence = "";
			clientSentence = inFromClient.readLine();
			
			
			//BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
			//clientSentence = inFromUser.readLine();
			Thread threadPIOne = new Thread( new SendToAllWorkers(clientSentence,"pi10.fu.campus",10));
			Thread threadPITwo = new Thread( new SendToAllWorkers(clientSentence,"pi11.fu.campus",11));
			Thread threadPIThree = new Thread( new SendToAllWorkers(clientSentence,"pi12.fu.campus",12));
			Thread threadPIFour = new Thread( new SendToAllWorkers(clientSentence,"pi13.fu.campus",13));
			Thread threadPIFive = new Thread( new SendToAllWorkers(clientSentence,"pi14.fu.campus",14));
			Thread threadPISix = new Thread( new SendToAllWorkers(clientSentence,"pi15.fu.campus",15));
			Thread threadPISeven = new Thread( new SendToAllWorkers(clientSentence,"pi16.fu.campus",16));
			Thread threadPIEight = new Thread( new SendToAllWorkers(clientSentence,"pi17.fu.campus",17));
			Thread threadPINine = new Thread( new SendToAllWorkers(clientSentence,"pi18.fu.campus",18));
			Thread threadPITen = new Thread( new SendToAllWorkers(clientSentence,"pi19.fu.campus",19));
			Thread threadPIEleven = new Thread( new SendToAllWorkers(clientSentence,"pi20.fu.campus",20));
			Thread threadPITwelve = new Thread( new SendToAllWorkers(clientSentence,"pi21.fu.campus",21));
			Thread threadPIThirteen = new Thread( new SendToAllWorkers(clientSentence,"pi22.fu.campus",22));
			Thread threadPIFourteen = new Thread( new SendToAllWorkers(clientSentence,"pi23.fu.campus",23));
			Thread threadPIFifteen = new Thread( new SendToAllWorkers(clientSentence,"pi24.fu.campus",24));
			Thread threadPISixteen = new Thread( new SendToAllWorkers(clientSentence,"pi25.fu.campus",25));
			Thread threadPIThirtyOne = new Thread( new SendToAllWorkers(clientSentence,"pi26.fu.campus",26));
			Thread threadPISeventeen = new Thread( new SendToAllWorkers(clientSentence,"pi27.fu.campus",27));
			Thread threadPIEighteen = new Thread( new SendToAllWorkers(clientSentence,"pi28.fu.campus",28));
			Thread threadPINineteen = new Thread( new SendToAllWorkers(clientSentence,"pi29.fu.campus",29));
			Thread threadPITwenty = new Thread( new SendToAllWorkers(clientSentence,"pi30.fu.campus",30));
			//Thread threadPIOTwentyOne = new Thread( new SendToAllWorkers(clientSentence,"pi31.fu.campus",31));
			Thread threadPITwentyTwo = new Thread( new SendToAllWorkers(clientSentence,"pi32.fu.campus",32));
			Thread threadPITwentyThree = new Thread( new SendToAllWorkers(clientSentence,"pi33.fu.campus",33));
			Thread threadPITwentyFour = new Thread( new SendToAllWorkers(clientSentence,"pi34.fu.campus",34));
			Thread threadPITwentyFive = new Thread( new SendToAllWorkers(clientSentence,"pi35.fu.campus",35));
			Thread threadPITwentySix = new Thread( new SendToAllWorkers(clientSentence,"pi36.fu.campus",36));
			Thread threadPITwentySeven = new Thread( new SendToAllWorkers(clientSentence,"pi37.fu.campus",37));
			Thread threadPITwentyEight = new Thread( new SendToAllWorkers(clientSentence,"pi38.fu.campus",38));
			Thread threadPITwentyNine = new Thread( new SendToAllWorkers(clientSentence,"pi39.fu.campus",39));
			Thread threadPIThirty = new Thread( new SendToAllWorkers(clientSentence,"pi40.fu.campus",40));
			
			//Start the threads (node 31 is dead - threadPIwentyOne has not been created)
			threadPIOne.start();
			threadPITwo.start();
			threadPIThree.start();
			threadPIFour.start();
			threadPIFive.start();
			threadPISix.start();
			threadPISeven.start();
			threadPIEight.start();
			threadPINine.start();
			threadPITen.start();
			threadPIEleven.start();
			threadPITwelve.start();
			threadPIThirteen.start();
			threadPIFourteen.start();
			threadPIFifteen.start();
			threadPISixteen.start();
			threadPIThirtyOne.start();
			threadPISeventeen.start();
			threadPIEighteen.start();
			threadPINineteen.start();
			threadPITwenty.start();
			//threadPITwentyOne.start();
			threadPITwentyTwo.start();
			threadPITwentyThree.start();
			threadPITwentyFour.start();
			threadPITwentyFive.start();
			threadPITwentySix.start();
			threadPITwentySeven.start();
			threadPITwentyEight.start();
			threadPITwentyNine.start();
			threadPIThirty.start();

			//Concatenate all sentences into one so it can be sent back to client
			//use "/" as delimiter
			String concatSentences = "";
			for(String s: returnFromWorkers){
				concatSentences =concatSentences.concat(s + "/");
			}
			
			//Return data to client
			outToClient.writeBytes(concatSentences + "\n");
		}
	}
	
	/*
	 * Sends UDP packet to a worker node and waits for response packet.
	 * @param: String input message that will be sent
	 * @param: String IPAddr the ip address of the destination node
	 * @output: String ouput message that is returned from the worker
	 */
	public synchronized static String sendToWorker(String input,String IPAddr) throws Exception{
		DatagramSocket clientSocket = new DatagramSocket();
		clientSocket.setSoTimeout(2000);
		InetAddress IPAddress = InetAddress.getByName(IPAddr);
		byte[] sendData = new byte[1024];
		byte[] receiveData = new byte[1024];
		sendData = input.getBytes();
		DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,IPAddress,9875);
		clientSocket.send(sendPacket);
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		try{
			clientSocket.receive(receivePacket);
			String output = new String(receivePacket.getData());
			output = output.trim();
			System.out.println("from worker: " + output);
			return output;
		}
		catch(Exception e){
			return "No response from worker";
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Pulled from mac
		//And from pc
		try{
			Timer timer = new Timer();
			timer.schedule(new RefreshConnections(), 0, 30000);
			receiveAndReturn();
		}
		catch(Exception e){
		}
		
	}

}
