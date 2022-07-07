/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <stack>

#include "hotstuff/util.h"
#include "hotstuff/consensus.h"
#include "hotstuff/graph.h"

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN
#define LOG_PROTO HOTSTUFF_LOG_PROTO

namespace hotstuff {

/* The core logic of HotStuff, is fairly simple :). */
/*** begin HotStuff protocol logic ***/
HotStuffCore::HotStuffCore(ReplicaID id,
                            privkey_bt &&priv_key):
        b0(new Block(true, 1)),
        b_lock(b0),
        b_exec(b0),
        vheight(0),
        priv_key(std::move(priv_key)),
        tails{b0},
        vote_disabled(false),
        id(id),
        storage(new EntityStorage()) {
    storage->add_blk(b0);
}

void HotStuffCore::sanity_check_delivered(const block_t &blk) {
    if (!blk->delivered)
        throw std::runtime_error("block not delivered");
}

block_t HotStuffCore::get_delivered_blk(const uint256_t &blk_hash) {
    block_t blk = storage->find_blk(blk_hash);
    if (blk == nullptr || !blk->delivered)
        throw std::runtime_error("block not delivered");
    return blk;
}

bool HotStuffCore::on_deliver_blk(const block_t &blk) {
    if (blk->delivered)
    {
        LOG_WARN("attempt to deliver a block twice");
        return false;
    }
    blk->parents.clear();
    for (const auto &hash: blk->parent_hashes)
        blk->parents.push_back(get_delivered_blk(hash));
    blk->height = blk->parents[0]->height + 1;

    if (blk->qc)
    {
        block_t _blk = storage->find_blk(blk->qc->get_obj_hash());
        if (_blk == nullptr)
            throw std::runtime_error("block referred by qc not fetched");
        blk->qc_ref = std::move(_blk);
    } // otherwise blk->qc_ref remains null

    for (auto pblk: blk->parents) tails.erase(pblk);
    tails.insert(blk);

    blk->delivered = true;
    LOG_DEBUG("deliver %s", std::string(*blk).c_str());
    return true;
}

void HotStuffCore::update_hqc(const block_t &_hqc, const quorum_cert_bt &qc) {
    if (_hqc->height > hqc.first->height)
    {
        hqc = std::make_pair(_hqc, qc->clone());
        on_hqc_update();
    }
}

// TODO: Themis
void HotStuffCore::update(const block_t &nblk) {
    /* nblk = b*, blk2 = b'', blk1 = b', blk = b */
    HOTSTUFF_LOG_INFO("[[update Start]] [R-%d] [L-]", get_id());
#ifndef HOTSTUFF_TWO_STEP
    /* three-step HotStuff */
    const block_t &blk2 = nblk->qc_ref;
    if (blk2 == nullptr) return;
    HOTSTUFF_LOG_INFO("[[update 1]]");
    /* decided blk could possible be incomplete due to pruning */
    if (blk2->decision) return;
    HOTSTUFF_LOG_INFO("[[update 2]]");
    update_hqc(blk2, nblk->qc);

    const block_t &blk1 = blk2->qc_ref;
    if (blk1 == nullptr) return;
    HOTSTUFF_LOG_INFO("[[update 3]]");
    if (blk1->decision) return;
    HOTSTUFF_LOG_INFO("[[update 4]]");
    if (blk1->height > b_lock->height) b_lock = blk1;

    const block_t &blk = blk1->qc_ref;
    if (blk == nullptr) return;
    HOTSTUFF_LOG_INFO("[[update 5]]");
    if (blk->decision) return;
    HOTSTUFF_LOG_INFO("[[update 6]]");

    /* commit requires direct parent */
    if (blk2->parents[0] != blk1 || blk1->parents[0] != blk) return;
    HOTSTUFF_LOG_INFO("[[update 7]]");
#else
    /* two-step HotStuff */
    const block_t &blk1 = nblk->qc_ref;
    if (blk1 == nullptr) return;
    if (blk1->decision) return;
    update_hqc(blk1, nblk->qc);
    if (blk1->height > b_lock->height) b_lock = blk1;

    const block_t &blk = blk1->qc_ref;
    if (blk == nullptr) return;
    if (blk->decision) return;

    /* commit requires direct parent */
    if (blk1->parents[0] != blk) return;
#endif
    /* b0 - - - - -> blk -> blk1 -> blk2 */
    /* otherwise commit */
    std::vector<block_t> commit_queue;
    block_t b;
    for (b = blk; b->height > b_exec->height; b = b->parents[0])
    { /* TODO: also commit the uncles/aunts */
        commit_queue.push_back(b);
    }
    if (b != b_exec)
        throw std::runtime_error("safety breached :( " +
                                std::string(*blk) + " " +
                                std::string(*b_exec));
    for (auto it = commit_queue.rbegin(); it != commit_queue.rend(); it++)
    {
        const block_t &blk = *it;

        // Themis
        auto const &order = fair_finalize(blk, nblk->get_e_update());
        if(order.empty() && !blk->get_graph().empty()) {
            /* this is not a tournament graph: stop looking at further blocks */
            break;
        }

        // Themis
        blk->decision = 1;
        do_consensus(blk);
        LOG_PROTO("commit %s", std::string(*blk).c_str());
        size_t n = order.size();
        for (size_t i=0; i<n; i++) {
            do_decide(Finality(id, 1, i, blk->height, order[i], blk->get_hash()));
        }
        b_exec = *it;

        HOTSTUFF_LOG_INFO("[[update Decided]] [R-%d] [L-]", get_id());

        // blk->decision = 1;
        // do_consensus(blk);
        // LOG_PROTO("commit %s", std::string(*blk).c_str());
        // for (size_t i = 0; i < blk->cmds.size(); i++)
        //     do_decide(Finality(id, 1, i, blk->height,
        //                         blk->cmds[i], blk->get_hash()));
    }
    // b_exec = blk;                        // Themis
    HOTSTUFF_LOG_INFO("[[update Ends]] [R-%d] [L-]", get_id());
}

// Themis
std::vector<uint256_t> HotStuffCore::
                        fair_finalize(block_t const &blk, 
                        std::vector<std::pair<uint256_t, uint256_t>> const &e_update){
    auto &graph = blk->get_graph();
    
    /** (1) For all Bi and transactions tx, tx0 in Bi that do not have an edge between them, 
     * if (tx; tx0) is in some Bj.e_update, then add that edge to Bi.G **/
    for(auto const &edge: e_update){
        if(graph.count(edge.first)>0 && graph.count(edge.second)>0){
            /* nodes exists but edge does not exists: update these edges */
            blk->update_graph(edge);
        }
    }

    /** (2) is graph B.G is a tournament **/
    if(!blk->is_tournament_graph()){
        /* Graph is not a tournament graph */
        return std::vector<uint256_t>();
    }

    /** (3) Compute the condensation graph of B.G, and topological sorting S **/
    CondensationGraph util_obj = CondensationGraph(blk->get_graph());
    auto const &sccs = util_obj.get_condensation_graph();
    /* Finalize a Global Order */
    /* TODO: Handle condorset cycles */
    std::vector<uint256_t> order;
    for (auto const &scc: sccs) {
        for (auto const &cmd: scc) {
            order.push_back(cmd);
        }
    }
    return order;
}



block_t HotStuffCore::on_propose(/* const std::vector<uint256_t> &cmds,*/               // Themis
                            const std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &graph,
                            std::vector<std::pair<uint256_t, uint256_t>> &e_update,
                            const std::vector<block_t> &parents,
                            bytearray_t &&extra) {
    if (parents.empty())
        throw std::runtime_error("empty parents");
    for (const auto &_: parents) tails.erase(_);
    /* create the new block */
    block_t bnew = storage->add_blk(
        new Block(parents, /*cmds,*/ graph, e_update,
            hqc.second->clone(), std::move(extra),
            parents[0]->height + 1,
            hqc.first,
            nullptr
        ));

    if(bnew->get_qc_ref()==nullptr){
        HOTSTUFF_LOG_INFO("[[on_propose]] [R-%d] [L-]  block hqc = %.10s, Decision = %d", get_id(), "null", bnew->get_decision());
    }
    else{
        HOTSTUFF_LOG_INFO("[[on_propose]] [R-%d] [L-] block hqc = %.10s, Decision = %d", get_id(), get_hex(bnew->get_qc_ref()->get_hash()).c_str(), bnew->get_decision());
    }
    
    const uint256_t bnew_hash = bnew->get_hash();
    bnew->self_qc = create_quorum_cert(bnew_hash);
    on_deliver_blk(bnew);
    update(bnew);
    Proposal prop(id, bnew, nullptr);
    LOG_PROTO("propose %s", std::string(*bnew).c_str());
    if (bnew->height <= vheight)
        throw std::runtime_error("new block should be higher than vheight");
    /* self-receive the proposal (no need to send it through the network) */
    for ( auto const &g: bnew->get_graph()) {
        HOTSTUFF_LOG_INFO("[[on_propose]] [R-%d] [L-%d] key = %.10s", get_id(), prop.proposer, get_hex(g.first).c_str());
        for (auto const &tx: g.second){
            HOTSTUFF_LOG_INFO("[[on_propose]] [R-%d] [L-%d] val = %.10s", get_id(), prop.proposer, get_hex(tx).c_str());
        }
    }
    on_receive_proposal(prop);
    on_propose_(prop);
    /* boradcast to other replicas */
    do_broadcast_proposal(prop);
    return bnew;
}

void HotStuffCore::on_receive_proposal(const Proposal &prop) {
    LOG_PROTO("got %s", std::string(prop).c_str());
    bool self_prop = prop.proposer == get_id();
    block_t bnew = prop.blk;

    auto const &graph = bnew->get_graph();
    for ( auto const &g: graph) {
        HOTSTUFF_LOG_INFO("[[on_receive_proposal Start]] [R-%d] [L-%d] key = %.10s", get_id(), prop.proposer, get_hex(g.first).c_str());
        for (auto const &tx: g.second){
            HOTSTUFF_LOG_INFO("[[on_receive_proposal Start]] [R-%d] [L-%d] val = %.10s", get_id(), prop.proposer, get_hex(tx).c_str());
        }
    }

    if (!self_prop)
    {
        sanity_check_delivered(bnew);
        HOTSTUFF_LOG_INFO("[[on_receive_proposal Before Update]] [R-%d] [L-%d]", get_id(), prop.proposer);
        update(bnew);
        HOTSTUFF_LOG_INFO("[[on_receive_proposal After Update]] [R-%d] [L-%d]", get_id(), prop.proposer);
    }
    bool opinion = false;
    if (bnew->height > vheight)
    {
        if (bnew->qc_ref && bnew->qc_ref->height > b_lock->height)
        {
            opinion = true; // liveness condition
            vheight = bnew->height;
        }
        else
        {   // safety condition (extend the locked branch)
            block_t b;
            for (b = bnew;
                b->height > b_lock->height;
                b = b->parents[0]);
            if (b == b_lock) /* on the same branch */
            {
                opinion = true;
                vheight = bnew->height;
            }
        }
    }
    LOG_PROTO("now state: %s", std::string(*this).c_str());
    if (!self_prop && bnew->qc_ref)
        on_qc_finish(bnew->qc_ref);
    on_receive_proposal_(prop);
    if (opinion && !vote_disabled){
        HOTSTUFF_LOG_INFO("[[on_receive_proposal Start Vote]] [R-%d] [L-%d]", get_id(), prop.proposer);
        do_vote(prop.proposer,
            Vote(id, bnew->get_hash(),
                create_part_cert(*priv_key, bnew->get_hash()), this));
    }
        
}

void HotStuffCore::on_receive_vote(const Vote &vote) {
    LOG_PROTO("got %s", std::string(vote).c_str());
    LOG_PROTO("now state: %s", std::string(*this).c_str());
    block_t blk = get_delivered_blk(vote.blk_hash);
    assert(vote.cert);
    size_t qsize = blk->voted.size();
    if (qsize >= config.nmajority) return;
    if (!blk->voted.insert(vote.voter).second)
    {
        LOG_WARN("duplicate vote for %s from %d", get_hex10(vote.blk_hash).c_str(), vote.voter);
        return;
    }
    auto &qc = blk->self_qc;
    if (qc == nullptr)
    {
        LOG_WARN("vote for block not proposed by itself");
        qc = create_quorum_cert(blk->get_hash());
    }
    qc->add_part(vote.voter, *vote.cert);
    if (qsize + 1 == config.nmajority)
    {
        qc->compute();
        update_hqc(blk, qc);
        on_qc_finish(blk);
    }
}

// Themis
void HotStuffCore::on_local_order (ReplicaID proposer, const std::vector<uint256_t> &cmds) {
    // TODO: Themis identify previously missing edges
    std::vector<std::pair<uint256_t, uint256_t>> l_update;
    /** create LocalOrder struct Object **/
    LocalOrder local_order = LocalOrder(get_id(), cmds, l_update, this);
    /** send local order to leader **/
    HOTSTUFF_LOG_INFO("[[on_local_order]] [R-%d] [L-%d] LocalOrder Object = %s", get_id(), proposer, local_order);
    do_send_local_order(proposer, local_order);
}

// Themis
void HotStuffCore::on_receive_local_order (const LocalOrder &local_order, const std::vector<block_t> &parents) {
    LOG_PROTO("got %s", std::string(local_order).c_str());
    LOG_PROTO("now state: %s", std::string(*this).c_str());

    HOTSTUFF_LOG_INFO("[[on_receive_local_order]] [fromR-%d] [thisL-%d] Receive LocalOrder on Leader (first ordered hash)= %.10s", local_order.initiator, get_id(), get_hex(local_order.ordered_hashes[0]).c_str());


    // TODO: if this is not a leader then ignore the request/ message 

    /** wait for majority of replicas **/
    size_t qsize = storage->get_local_order_cache_size();
    if(qsize >= config.nmajority) { return; }
    
    /** add new local order to the storage **/
    storage->add_local_order(local_order.initiator, local_order.ordered_hashes, local_order.l_update);

    /** Trigger FairPropose() and FairUpdate() **/
    qsize = storage->get_local_order_cache_size();
    if(qsize == config.nmajority){
        /* FairPropose() */
        std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph = fair_propose();
        // TODO: Themis FairUpdate()
        std::vector<std::pair<uint256_t, uint256_t>> e_update;
        /** Create a new proposal block and broadcast to the replicas **/
        on_propose(graph, e_update, parents);
    }
}

// Themis
std::unordered_map<uint256_t, std::unordered_set<uint256_t>> HotStuffCore::fair_propose() {
    HOTSTUFF_LOG_INFO("[[fairPropose START]] [R-%d]", get_id());
    /** (1) get those replicas from which Leader has received their local order **/
    std::vector<ReplicaID> replicas = storage->get_local_order_replia_vector();

    /** (2) Create an empty graph G = (V,E) **/
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;

    /** (3) For each non-blank tx, add a vertex tx to V **/
    std::unordered_map<uint256_t, uint16_t> tx_count;
    /* find transaction count */
    for(ReplicaID replica: replicas){
        for(uint256_t tx_hash: storage->get_ordered_hash_vector(replica)){
            tx_count[tx_hash]++;
        }
    }
    /* find non blank transactions and add them to the graph */
    std::unordered_set<uint256_t> solid_tx_set;
    for(auto &tx: tx_count){
        uint256_t hash = tx.first;
        double count = tx.second * 1.0;
        if(count >= config.non_blank_tx_threshold) {
            /** this is a non blank transaction **/
            graph.insert(std::make_pair(hash, std::unordered_set<uint256_t>()));
        }
        if(count >= config.solid_tx_threshold) {
            solid_tx_set.insert(hash);
        }
    }

    /** (4) Add edges to E **/
    std::unordered_map<uint256_t, std::unordered_map<uint256_t, uint16_t>> edge_count;
    /* Find edge count */
    for(ReplicaID replica: replicas){
        std::vector<uint256_t> ordered_hash = storage->get_ordered_hash_vector(replica);
        size_t len = ordered_hash.size();
        for(size_t from=0; from<len; from++) {
            for(size_t to=from+1; to<len; to++){
                edge_count[ordered_hash[from]][ordered_hash[to]]++;
            }
        }
    }
    /* add edges where k>=n(1-gama)+f+1 {>= tx_edge_threshold} */
    for(auto &from : edge_count){
        for(auto &to : from.second){
            uint256_t from_v = from.first;
            uint256_t to_v = to.first;
            uint16_t occurance = to.second;
            if(1.0 * occurance >= config.tx_edge_threshold
                && graph[to_v].count(from_v)==0){ 
                    /* edge occurance is above threshold and no reverse edge is already present in graph */
                    graph[from_v].insert(to_v);
                }
        }
    }
    /** (5) Compute the condensation graph G* **/
    CondensationGraph utility_obj = CondensationGraph(graph);
    std::vector<std::vector<uint256_t>> topo_sorted_cond_graph = utility_obj.get_condensation_graph();

    for (auto const &scc: topo_sorted_cond_graph){
        HOTSTUFF_LOG_INFO("[[fairPropose SCC start]] [R-%d]", get_id());
        for(auto const &tx: scc){
            HOTSTUFF_LOG_INFO("[[fairPropose SCC]] [R-%d] tx = %.10s", get_id(), get_hex(tx).c_str());
        }
    }
    

    /** (6) Find the Last vertex `V` in S that has solid transaction. **/
    size_t n_scc = topo_sorted_cond_graph.size();
    int scc_i=0;
    for( ; scc_i<n_scc; scc_i++){
        bool solid_found=false;
        for(auto const &tx: topo_sorted_cond_graph[scc_i]){
            if(solid_tx_set.count(tx)>0){
                solid_found = true;
                break;
            }
        }
        if(!solid_found){
            break;
        }
    }

    /** (7) Remove those transactions from G, that are part of vertices after V in S **/
    for( ; scc_i<n_scc; scc_i++){
        for(auto const &tx: topo_sorted_cond_graph[scc_i]){
            /* remove this tx from original graph */
            graph.erase(tx);
            for(auto const &child: utility_obj.get_decendents_from_transposed_graph(tx)){
                graph[child].erase(tx);
            }
        }
    }

    /** (8) Output G **/
    for ( auto const &g: graph) {
        HOTSTUFF_LOG_INFO("[[fairPropose END]] [R-%d] key = %.10s", get_id(), get_hex(g.first).c_str());
        for (auto const &tx: g.second){
            HOTSTUFF_LOG_INFO("[[fairPropose END]] [R-%d] val = %.10s", get_id(), get_hex(tx).c_str());
        }
    }
    return graph;
}

/*** end HotStuff protocol logic ***/
void HotStuffCore::on_init(uint32_t nfaulty, double fairness_parameter) {   // Themis
    config.nmajority = config.nreplicas - nfaulty;
    config.fairness_parameter = fairness_parameter;                 // Themis
    /** Do not switch below 3 statements with above 2 statements **/
    config.solid_tx_threshold = get_solid_tx_threshold();           // Themis
    config.non_blank_tx_threshold = get_non_blank_tx_threshold();   // Themis
    config.tx_edge_threshold = get_tx_edge_threshold();             // Themis
    HOTSTUFF_LOG_INFO("[[on_init]] [R-%d]  nmajority = %d, fairness_parameter = %f, solid_tx_threshold = %f, non_blank_tx_threshold = %f, tx_edge_threshold = %f", get_id(), config.nmajority, config.fairness_parameter, config.solid_tx_threshold, config.non_blank_tx_threshold, config.tx_edge_threshold);
    b0->qc = create_quorum_cert(b0->get_hash());
    b0->qc->compute();
    b0->self_qc = b0->qc->clone();
    b0->qc_ref = b0;
    hqc = std::make_pair(b0, b0->qc->clone());
}

// Themis
double HotStuffCore::get_solid_tx_threshold() {
    size_t nmajority = config.nmajority;
    size_t n = config.nreplicas;
    size_t f = n - nmajority;
    return n - 2.0*f;
}

// Themis
double HotStuffCore::get_non_blank_tx_threshold() {
    size_t nmajority = config.nmajority;
    size_t n = config.nreplicas;
    size_t f = n - nmajority;
    double gama = config.fairness_parameter;
    double solid = n - 2.0*f;
    double shaded = n * (1.0-gama) + f + 1.0;
    return solid > shaded ? shaded : solid;
}

// Themis
double HotStuffCore::get_tx_edge_threshold() {
    size_t nmajority = config.nmajority;
    size_t n = config.nreplicas;
    size_t f = n - nmajority;
    double gama = config.fairness_parameter;
    return n * (1.0-gama) + f + 1.0;
}

void HotStuffCore::prune(uint32_t staleness) {
    block_t start;
    /* skip the blocks */
    for (start = b_exec; staleness; staleness--, start = start->parents[0])
        if (!start->parents.size()) return;
    std::stack<block_t> s;
    start->qc_ref = nullptr;
    s.push(start);
    while (!s.empty())
    {
        auto &blk = s.top();
        if (blk->parents.empty())
        {
            storage->try_release_blk(blk);
            s.pop();
            continue;
        }
        blk->qc_ref = nullptr;
        s.push(blk->parents.back());
        blk->parents.pop_back();
    }
}

void HotStuffCore::add_replica(ReplicaID rid, const PeerId &peer_id,
                                pubkey_bt &&pub_key) {
    config.add_replica(rid,
            ReplicaInfo(rid, peer_id, std::move(pub_key)));
    b0->voted.insert(rid);
}

promise_t HotStuffCore::async_qc_finish(const block_t &blk) {
    if (blk->voted.size() >= config.nmajority)
        return promise_t([](promise_t &pm) {
            pm.resolve();
        });
    auto it = qc_waiting.find(blk);
    if (it == qc_waiting.end())
        it = qc_waiting.insert(std::make_pair(blk, promise_t())).first;
    return it->second;
}

void HotStuffCore::on_qc_finish(const block_t &blk) {
    auto it = qc_waiting.find(blk);
    if (it != qc_waiting.end())
    {
        it->second.resolve();
        qc_waiting.erase(it);
    }
}

promise_t HotStuffCore::async_wait_proposal() {
    return propose_waiting.then([](const Proposal &prop) {
        return prop;
    });
}

promise_t HotStuffCore::async_wait_receive_proposal() {
    return receive_proposal_waiting.then([](const Proposal &prop) {
        return prop;
    });
}

promise_t HotStuffCore::async_hqc_update() {
    return hqc_update_waiting.then([this]() {
        return hqc.first;
    });
}

void HotStuffCore::on_propose_(const Proposal &prop) {
    auto t = std::move(propose_waiting);
    propose_waiting = promise_t();
    t.resolve(prop);
}

void HotStuffCore::on_receive_proposal_(const Proposal &prop) {
    auto t = std::move(receive_proposal_waiting);
    receive_proposal_waiting = promise_t();
    t.resolve(prop);
}

void HotStuffCore::on_hqc_update() {
    auto t = std::move(hqc_update_waiting);
    hqc_update_waiting = promise_t();
    t.resolve();
}

HotStuffCore::operator std::string () const {
    DataStream s;
    s << "<hotstuff "
      << "hqc=" << get_hex10(hqc.first->get_hash()) << " "
      << "hqc.height=" << std::to_string(hqc.first->height) << " "
      << "b_lock=" << get_hex10(b_lock->get_hash()) << " "
      << "b_exec=" << get_hex10(b_exec->get_hash()) << " "
      << "vheight=" << std::to_string(vheight) << " "
      << "tails=" << std::to_string(tails.size()) << ">";
    return s;
}

}
