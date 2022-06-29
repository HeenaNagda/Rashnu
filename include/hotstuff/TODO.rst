(1*) propose_handler = local_order_handler
(2*) do_broadcast_proposal = do_send_local_order
(3*) struct LocalOrder: public Serializable : whether the arraylist should be passed by reference / value ? 
(4*) Proposal = LocalOrder
(5) What is promise ???
(6) do_send_local_order : about pmaker what to do?
(7) on_local_order : identify previously missing edges
(8) local_order_handler : add FairPropose, FairUpdate
(9) Wait for time or block size on replicas

- create message and struct format for the local orderings [finaliza the data structure, seriazation deserialization]
- Replica: Receive cmds from client. waits for number of command of size block
- Create a message with this new command block and send to the Leader
- Created handler on leader side to receive the replica messages.
- [Currently Working] Leader waits for majority replicas (local order message) before making decision
- FairPropose, FairUpdate