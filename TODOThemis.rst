(1*) propose_handler = local_order_handler
(2*) do_broadcast_proposal = do_send_local_order
(3*) struct LocalOrder: public Serializable : whether the arraylist should be passed by reference / value ? 
(4*) Proposal = LocalOrder
(5) What is promise ???
(6) do_send_local_order : about pmaker what to do?
(7) on_local_order : identify previously missing edges
(8*) local_order_handler : add FairPropose, FairUpdate
(9*) Wait for time or block size on replicas
(10) Replicas must include the tx that is not in proposal in last rounds.
(11) cmd_idx in Finality() is just make the object hash unique or something else ??? 
(12) FairFinalize(): Should we check edges from every block or only from new block (b/s of 2/3 step hotstuff) ???
(13) FairPropose(): Themis For all tx and tx' that are part of the same leader proposal ???
