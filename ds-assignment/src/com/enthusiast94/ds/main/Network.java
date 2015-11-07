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
    private static final int PERIOD = 20;
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
            parseFile("input.txt");
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

            if (messagesToDeliver.size() == 0 && electionMessagesPerRound.size() == 0 && failMessages.size() == 0) {
                shouldEnd = true;
            } else if (messagesToDeliver.size() == 0 && electionMessagesPerRound.size() == 0) {
                processFailureMessageQueue();
            }

            deliverMessages();

            try {
                Thread.sleep(PERIOD);
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

        Map<Integer, List<Integer>> neighbouringNodeIdsMap = new HashMap<>();
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
                List<Integer> neighbouringNodeIds = new ArrayList<>();

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
                            mainNode.setPrevious(previous);
                        }

                        previous = mainNode;
                    } else {
                        int neighbouringNodeId = Integer.valueOf(nodeIds[i]);
                        neighbouringNodeIds.add(neighbouringNodeId);
                    }
                }

                neighbouringNodeIdsMap.put(mainNodeId, neighbouringNodeIds);

                // set first node to be the next node of last node and
                // set last node to be the previous node of first node
                if (previous != null) {
                    previous.setNext(first);
                    first.setPrevious(previous);
                }
            }
        }

        // Now add neighbours for each of the nodes in the ring.
        // This needs to be done AFTER the ring topology has been constructed
        // in order to prevent multiple Node instances with the same node id.
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            for (int nodeId : neighbouringNodeIdsMap.get(entry.getKey())) {
                entry.getValue().addNeighbour(nodes.get(nodeId));
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
                nodes.get(nodeId).startElection();
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

    public synchronized void processFailureMessageQueue() {
        FailMessage failMessage = failMessages.get(0);
        System.out.println("Node " + failMessage.getFailedNodeId() + " FAILED");

        Node failedNode = nodes.get(failMessage.getFailedNodeId());

        for (Node failedNodeNeighbour : failedNode.getNeighbours()) {
            failedNodeNeighbour.informNeighbourFailure(failedNode.getNodeId());
        }

        // stop failed node's thread
        failedNode.stopNode();

        failMessages.remove(failMessage);
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
