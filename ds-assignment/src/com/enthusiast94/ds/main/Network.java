package com.enthusiast94.ds.main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by manas on 05-11-2015.
 */
public class Network {

    private Map<Integer, Node> nodes;
    private int round;
    private int period = 20;
    private Map<Integer, String> messagesToDeliver; //Integer for the id of the sender and String for the message
    private Map<Integer, ElectMessage> electionMessagesPerRound;
    private List<FailMessage> failMessages;

    public Network() {
		/*
		Code to call methods for parsing the input file, initiating the system and producing the log can be added here.
		*/

        Logger.init("log.txt");
        messagesToDeliver = new HashMap<>();
        electionMessagesPerRound = new HashMap<>();
        nodes = new HashMap<>();
        failMessages = new ArrayList<>();
    }

    public void start() {
        try {
            parseFile("input_simple.txt");
        } catch (IOException e) {
            System.out.println("There was a problem parsing file");
            e.printStackTrace();
            System.exit(1);
        }

        boolean shouldEnd = false;

        // start all node threads
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            entry.getValue().start();
        }

        while (!shouldEnd) {
            round++;

            System.out.println("Round: " + round);

            collectMessages();

            if (messagesToDeliver.size() == 0 && electionMessagesPerRound.size() == 0) {
                shouldEnd = true;
            }

            deliverMessages();

            try {
                Thread.sleep(period);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // stop all node threads
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            entry.getValue().stopNode();
        }

        try {
            Logger.getInstance().logToFile();
        } catch (IOException e) {
            System.out.println("There was a problem logging to file");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void parseFile(String fileName) throws IOException {
   		/*
   		Code to parse the file can be added here. Notice that the method's descriptor must be defined.
   		*/

        File networkTextFile = new File(fileName);
        FileInputStream fis = new FileInputStream(networkTextFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));

        Node first = null;
        Node previous = null;
        boolean isFirst = true;
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("Node_id")) {
                // ignore
            } else if (line.startsWith("ELECT")) {
                String[] split = line.split("\\s");
                int round = Integer.valueOf(split[1]);
                List<Integer> nodeIds = new ArrayList<>();

                for (int i=2; i<split.length; i++) {
                    nodeIds.add(Integer.valueOf(split[i]));
                }

                electionMessagesPerRound.put(round, new ElectMessage(nodeIds));
            } else if (line.startsWith("FAIL")) {
                String[] split = line.split("\\s");
                failMessages.add(new FailMessage(Integer.valueOf(split[1])));
            } else {
                String[] nodeIds = line.split("\\s");
                int mainNodeId = Integer.valueOf(nodeIds[0]);

                for (int i=0; i<nodeIds.length; i++) {
                    if (i == 0) {
                        Node mainNode = new Node(mainNodeId);
                        nodes.put(mainNodeId, mainNode);

                        if (isFirst) {
                            first = mainNode;
                            isFirst = false;
                        }

                        if (previous != null) {
                            previous.setNext(mainNode);
                        }

                        previous = mainNode;
                    } else {
                        int neighbouringNodeId = Integer.valueOf(nodeIds[i]);
                        if (nodes.containsKey(neighbouringNodeId)) {
                            nodes.get(mainNodeId).addNeighbour(nodes.get(neighbouringNodeId));
                        } else {
                            Node neighbouringNode = new Node(neighbouringNodeId);
                            nodes.get(mainNodeId).addNeighbour(neighbouringNode);
                        }
                    }
                }

                // set first node to be the next node of last node
                if (previous != null) {
                    previous.setNext(first);
                }
            }
        }

        bufferedReader.close();
    }

    public synchronized void addMessage(int id, String m) {
		/*
		At each round, the network collects all the messages that the nodes want to send to their neighbours.
		Implement this logic here.
		*/

        messagesToDeliver.put(id, m);
    }

    public synchronized void collectMessages() {
        ElectMessage electMessageForCurrentRound = electionMessagesPerRound.get(round);
        if (electMessageForCurrentRound != null) {
            System.out.println("ELECTION STARTED");

            for (Integer nodeId : electMessageForCurrentRound.getNodeIds()) {
                nodes.get(nodeId).getOutgoingMessages().add(new Node.ElectionMessage(nodeId, nodeId).toString());
            }

            electionMessagesPerRound.remove(round);
        }

        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            // add the least recent incoming message to messagesToDeliver
            // this would prevent multiple messages being sent to a neighbour in the same round
            if (entry.getValue().getOutgoingMessages().size() > 0) {
                addMessage(entry.getValue().getNodeId(), entry.getValue().getOutgoingMessages().get(0));
            }
        }
    }

    public synchronized void deliverMessages() {
		/*
		At each round, the network delivers all the messages that it has collected from the nodes.
		Implement this logic here.
		The network must ensure that a node can send only to its neighbours, one message per round per neighbour.
		*/

        for (Map.Entry<Integer, String> entry : messagesToDeliver.entrySet()) {
            Node node = nodes.get(entry.getKey());
            String message = entry.getValue();

            node.sendMsg(message);
        }

        for (Map.Entry<Integer, String> entry : messagesToDeliver.entrySet()) {
            Node node = nodes.get(entry.getKey());
            String message = entry.getValue();

            node.getNext().receiveMsg(message);
        }

        messagesToDeliver.clear();
    }

    public synchronized void informNodeFailure(int id) {
		/*
		Method to inform the neighbours of a failed node about the event.
		*/
    }

    private static class ElectMessage {

        private List<Integer> nodeIds;

        public ElectMessage(List<Integer> nodeIds) {
            this.nodeIds = nodeIds;
        }

        public List<Integer> getNodeIds() {
            return nodeIds;
        }
    }

    private static class FailMessage {

        private int failedNodeId;

        public FailMessage(int failedNodeId) {
            this.failedNodeId = failedNodeId;
        }

        public int getFailedNodeId() {
            return failedNodeId;
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException {
		/*
		Your main must get the input file as input.
		*/

        new Network().start();
    }
}
