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
				MasterNodeThreaded.sendToWorker(blankMessage,"pi10.fu.campus",1,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi11.fu.campus",2,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi12.fu.campus",3,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi13.fu.campus",4,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi14.fu.campus",5,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi15.fu.campus",6,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi16.fu.campus",7,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi17.fu.campus",8,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi18.fu.campus",9,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi19.fu.campus",10,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi20.fu.campus",11,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi21.fu.campus",12,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi22.fu.campus",13,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi23.fu.campus",14,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi24.fu.campus",15,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi25.fu.campus",16,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi26.fu.campus",17,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi27.fu.campus",18,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi28.fu.campus",19,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi29.fu.campus",20,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi30.fu.campus",21,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi31.fu.campus",22,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi32.fu.campus",23,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi33.fu.campus",24,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi34.fu.campus",25,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi35.fu.campus",26,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi36.fu.campus",27,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi37.fu.campus",28,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi38.fu.campus",29,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi39.fu.campus",30,true);
				MasterNodeThreaded.sendToWorker(blankMessage,"pi40.fu.campus",31,true);
				
			}
			catch(Exception e){}
		}
	}
	
	/*
	 * Allows for a different thread for communication with each node.
	 */
	public static class SendToAllWorkers implements Runnable{
		
		 String IPAddr;
		 int piNum;
		 String clientSentence;
		
		public SendToAllWorkers(String clientSentenceIn, String IPAddrIn, int piNumIn){
			//ensureCorrectVars(clientSentenceIn,IPAddrIn,piNumIn);
			this.piNum = piNumIn-9;
			this.IPAddr = IPAddrIn;
			this.clientSentence = clientSentenceIn;
			
		}
		
		public void run(){
			try{
				sendToWorker(this.clientSentence,this.IPAddr,this.piNum,false);
				System.out.println(returnFromWorkers[piNum-1]);
			}
			catch(Exception e){
				System.out.println("run method failed");
			}
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
			Runnable threadPITen = new SendToAllWorkers(clientSentence,"pi10.fu.campus",10);
			Runnable threadPIEleven = new SendToAllWorkers(clientSentence,"pi11.fu.campus",11);
			Runnable threadPITwelve = new SendToAllWorkers(clientSentence,"pi12.fu.campus",12);
			Runnable threadPIThirteen = new SendToAllWorkers(clientSentence,"pi13.fu.campus",13);
			Runnable threadPIFourteen = new SendToAllWorkers(clientSentence,"pi14.fu.campus",14);
			Runnable threadPIFifteen = new SendToAllWorkers(clientSentence,"pi15.fu.campus",15);
			Runnable threadPISixteen = new SendToAllWorkers(clientSentence,"pi16.fu.campus",16);
			Runnable threadPISeventeen = new SendToAllWorkers(clientSentence,"pi17.fu.campus",17);
			Runnable threadPIEighteen = new SendToAllWorkers(clientSentence,"pi18.fu.campus",18);
			Runnable threadPINineteen = new SendToAllWorkers(clientSentence,"pi19.fu.campus",19);
			Runnable threadPITwenty = new SendToAllWorkers(clientSentence,"pi20.fu.campus",20);
			Runnable threadPITwentyOne = new SendToAllWorkers(clientSentence,"pi21.fu.campus",21);
			Runnable threadPITwentyTwo = new SendToAllWorkers(clientSentence,"pi22.fu.campus",22);
			Runnable threadPITwentyThree = new SendToAllWorkers(clientSentence,"pi23.fu.campus",23);
			Runnable threadPITwentyFour = new SendToAllWorkers(clientSentence,"pi24.fu.campus",24);
			Runnable threadPITwentyFive = new SendToAllWorkers(clientSentence,"pi25.fu.campus",25);
			Runnable threadPITwentySix = new SendToAllWorkers(clientSentence,"pi26.fu.campus",26);
			Runnable threadPITwentySeven = new SendToAllWorkers(clientSentence,"pi27.fu.campus",27);
			Runnable threadPITwentyEight = new SendToAllWorkers(clientSentence,"pi28.fu.campus",28);
			Runnable threadPITwentyNine = new SendToAllWorkers(clientSentence,"pi29.fu.campus",29);
			Runnable threadPIThirty = new SendToAllWorkers(clientSentence,"pi30.fu.campus",30);
			Runnable threadPIThirtyOne = new SendToAllWorkers(clientSentence,"pi31.fu.campus",31);
			Runnable threadPIThirtyTwo = new SendToAllWorkers(clientSentence,"pi32.fu.campus",32);
			Runnable threadPIThirtyThree = new SendToAllWorkers(clientSentence,"pi33.fu.campus",33);
			Runnable threadPIThirtyFour= new SendToAllWorkers(clientSentence,"pi34.fu.campus",34);
			Runnable threadPIThirtyFive = new SendToAllWorkers(clientSentence,"pi35.fu.campus",35);
			Runnable threadPIThirtySix = new SendToAllWorkers(clientSentence,"pi36.fu.campus",36);
			Runnable threadPIThirtySeven = new SendToAllWorkers(clientSentence,"pi37.fu.campus",37);
			Runnable threadPIThirtyEight = new SendToAllWorkers(clientSentence,"pi38.fu.campus",38);
			Runnable threadPIThirtyNine = new SendToAllWorkers(clientSentence,"pi39.fu.campus",39);
			Runnable threadPIFourty = new SendToAllWorkers(clientSentence,"pi40.fu.campus",40);
			//Start the threads (node 31 is dead - threadPIwentyOne will be PI03)
			new Thread(threadPITen).start();
			new Thread(threadPIEleven).start();
			new Thread(threadPITwelve).start();
			new Thread(threadPIThirteen).start();
			new Thread(threadPIFourteen).start();
			new Thread(threadPIFifteen).start();
			new Thread(threadPISixteen).start();
			new Thread(threadPIThirtyOne).start();
			new Thread(threadPISeventeen).start();
			new Thread(threadPIEighteen).start();
			new Thread(threadPINineteen).start();
			new Thread(threadPITwenty).start();
			new Thread(threadPITwentyOne).start();
			new Thread(threadPITwentyTwo).start();
			new Thread(threadPITwentyThree).start();
			new Thread(threadPITwentyFour).start();
			new Thread(threadPITwentyFive).start();
			new Thread(threadPITwentySix).start();
			new Thread(threadPITwentySeven).start();
			new Thread(threadPITwentyEight).start();
			new Thread(threadPITwentyNine).start();
			new Thread(threadPIThirty).start();
			new Thread(threadPIThirtyOne).start();
			new Thread(threadPIThirtyTwo).start();
			new Thread(threadPIThirtyThree).start();
			new Thread(threadPIThirtyFour).start();
			new Thread(threadPIThirtyFive).start();
			new Thread(threadPIThirtySix).start();
			new Thread(threadPIThirtySeven).start();
			new Thread(threadPIThirtyEight).start();
			new Thread(threadPIThirtyNine).start();
			new Thread(threadPIFourty).start();
			
			System.out.println("ALL THREADS DONE FIRING");
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
	public static String sendToWorker(String input,String IPAddr,int piNum, boolean refresh) throws Exception{
		int designatedPort = 9800 + piNum;
		DatagramSocket clientSocket = new DatagramSocket();
		//System.out.println("trying to reach server at port: " + designatedPort);
		if(refresh){
			clientSocket.setSoTimeout(05);
		}
		else{
			clientSocket.setSoTimeout(4000);
		}
		InetAddress IPAddress = InetAddress.getByName(IPAddr);
		byte[] sendData = new byte[1024];
		byte[] receiveData = new byte[1024];
		sendData = input.getBytes();
		DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,IPAddress,designatedPort);
		clientSocket.send(sendPacket);
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		try{
			clientSocket.receive(receivePacket);
			String output = new String(receivePacket.getData());
			output = output.trim();
			returnFromWorkers[piNum-1] = output;
			//addToWorkerArray(output,piNum);
			return output;
		}
		catch(Exception e){
			return "No response from worker";
		}
	}
	
	public static synchronized void addToWorkerArray(String data,int index){
		returnFromWorkers[index-1]= data;
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
