package com.enthusiast94.ds.main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by manas on 05-11-2015.
 */
public class Node extends Thread {

    private int id;
    private boolean isParticipant;
    private boolean isLeader = false;
    private Node next;
    private Node previous;
    private int leaderId;
    private List<Node> neighbours;
    private List<String> incomingMessages; // Queue for the incoming messages
    private List<String> outgoingMessages;
    private boolean shouldStop = false;

    public Node(int id) {
        this.id = id;

        incomingMessages = Collections.synchronizedList(new ArrayList<>());
        outgoingMessages = Collections.synchronizedList(new ArrayList<>());
        neighbours = new ArrayList<>();
        leaderId = -1;
    }

    public int getNodeId() {
		/*
		Method to get the Id of a node instance
		*/

        return id;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getPrevious() {
        return previous;
    }

    public void setPrevious(Node previous) {
        this.previous = previous;
    }

    public List<Node> getNeighbours() {
        return neighbours;
    }

    public List<String> getIncomingMessages() {
        return incomingMessages;
    }

    public List<String> getOutgoingMessages() {
        return outgoingMessages;
    }

    public void addNeighbour(Node neighbour) {
        this.neighbours.add(neighbour);
    }

    public void receiveMsg(String m) {
		/*
		Method that implements the reception of an incoming message by a node
		*/

        incomingMessages.add(m);
    }

    private void processIncomingMessage(String m) {
        NodeMessage message = NodeMessageParser.parse(m);

        System.out.println("Node " + getNodeId() + " received '" + m + "'");

        if (message instanceof ElectionMessage) {
            ElectionMessage electionMessage = (ElectionMessage) message;

            if (!isParticipant) {
                m = new ElectionMessage(electionMessage.getElectionStarterNodeId(),
                        Math.max(electionMessage.getMessageNodeId(), id)).toString();
                outgoingMessages.add(m);

                isParticipant = true;
            } else {
                if (electionMessage.getMessageNodeId() == id) {
                    isLeader = true;
                    leaderId = id;
                    outgoingMessages.add(new LeaderMessage(electionMessage.getElectionStarterNodeId(), leaderId).toString());
                    isParticipant = false;
                    System.out.println("LEADER " + id);
                    Logger.getInstance().addMessage("LEADER " + id);
                } else if (electionMessage.getMessageNodeId() > id) {
                    outgoingMessages.add(m);
                }
            }
        } else if (message instanceof LeaderMessage) {
            LeaderMessage leaderMessage = (LeaderMessage) message;

            leaderId = leaderMessage.getLeaderNodeId();

            // When a process receives an elected message, it marks itself as non-participant,
            // records the elected UID, and forwards the elected message unchanged.
            isParticipant = false;

            if (id != leaderId) {
                outgoingMessages.add(m);
            } else {
                // When the elected message reaches the newly elected leader, the leader discards that message, and the election is over.
                System.out.println("ELECTION ENDED");
            }
        } else if (message instanceof NeighbourFailureMessage) {
            NeighbourFailureMessage neighbourFailureMessage = (NeighbourFailureMessage) message;

            // get failed node instance from neighbours list
            Node failedNode = null;
            for (Node node : neighbours) {
                if (node.getNodeId() == neighbourFailureMessage.getFailedNeighbourId()) {
                    failedNode = node;
                    break;
                }
            }

            // update next and previous node of next and previous nodes of failed node respectively
            assert failedNode != null;

            if (id == failedNode.getPrevious().getNodeId()) {
                next = failedNode.getNext();

                // add new next node to neighbours
                neighbours.add(failedNode.getNext());
            } else if (id == failedNode.getNext().getNodeId()) {
                previous = failedNode.getPrevious();

                // add new previous node to neighbours
                neighbours.add(failedNode.getPrevious());
            }

            // remove failed node from the neighbours
            neighbours.remove(failedNode);

            // if failed node was the leader, start leader election
            if (failedNode.isLeader) {
                startElection();
            }
        } else {
            throw new RuntimeException("invalid message type");
        }
    }

    public void sendMsg(String m) {
		/*
		Method that implements the sending of a message by a node.
		The message must be delivered to its recepients through the network.
		This method need only implement the logic of the network receiving an outgoing message from a node.
		The remainder of the logic will be implemented in the network class.
		*/

        NodeMessage message = NodeMessageParser.parse(m);

        if (message instanceof ElectionMessage) {
            isParticipant = true;
        }

        outgoingMessages.remove(m);
    }

    public void startElection() {
        outgoingMessages.add(new Node.ElectionMessage(id, id).toString());
    }

    public void informNeighbourFailure(int failedNeighbourId) {
        incomingMessages.add(new Node.NeighbourFailureMessage(failedNeighbourId).toString());
    }

    @Override
    public void run() {
        while (!shouldStop) {
            synchronized (incomingMessages) {
                for (String m : incomingMessages) {
                    processIncomingMessage(m);
                }

                if (incomingMessages.size() > 0) {
                    incomingMessages.clear();
                }
            }
        }
    }

    public void stopNode() {
        shouldStop = true;
    }

    private interface NodeMessage {}

    private static class ElectionMessage implements NodeMessage {

        private int electionStarterNodeId;
        private int messageNodeId;

        public ElectionMessage(int electionStarterNodeId, int messageNodeId) {
            this.electionStarterNodeId = electionStarterNodeId;
            this.messageNodeId = messageNodeId;
        }

        public int getElectionStarterNodeId() {
            return electionStarterNodeId;
        }

        public int getMessageNodeId() {
            return messageNodeId;
        }

        @Override
        public String toString() {
            return "ELECTION " + electionStarterNodeId + " " + messageNodeId;
        }
    }

    private static class LeaderMessage implements NodeMessage {

        private int electionStarterNodeId;
        private int leaderNodeId;

        public LeaderMessage(int electionStarterNodeId, int leaderNodeId) {
            this.electionStarterNodeId = electionStarterNodeId;
            this.leaderNodeId = leaderNodeId;
        }

        public int getElectionStarterNodeId() {
            return electionStarterNodeId;
        }

        public int getLeaderNodeId() {
            return leaderNodeId;
        }

        @Override
        public String toString() {
            return "LEADER " + electionStarterNodeId + " " + leaderNodeId;
        }
    }

    public static class NeighbourFailureMessage implements NodeMessage {

        private int failedNeighbourId;

        public NeighbourFailureMessage(int failedNeighbourId) {
            this.failedNeighbourId = failedNeighbourId;
        }

        public int getFailedNeighbourId() {
            return failedNeighbourId;
        }

        @Override
        public String toString() {
            return "NEIGHBOUR-FAILURE " + failedNeighbourId;
        }
    }

    public static class NodeMessageParser {
        public static NodeMessage parse(String message) {
            String[] split = message.split("\\s");

            switch (split[0]) {
                case "ELECTION":
                    return new ElectionMessage(Integer.valueOf(split[1]), Integer.valueOf(split[2]));
                case "LEADER":
                    return new LeaderMessage(Integer.valueOf(split[1]), Integer.valueOf(split[2]));
                case "NEIGHBOUR-FAILURE":
                    return new NeighbourFailureMessage(Integer.valueOf(split[1]));
                default:
                    return null;
            }
        }
    }
}
