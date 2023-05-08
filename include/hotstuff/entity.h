/**
 * Copyright 2018 VMware
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

#ifndef _HOTSTUFF_ENT_H
#define _HOTSTUFF_ENT_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstddef>
#include <ios>
#include <queue>
#include <deque>

#include "salticidae/netaddr.h"
#include "salticidae/ref.h"
#include "hotstuff/type.h"
#include "hotstuff/util.h"
#include "hotstuff/crypto.h"
#include "hotstuff/ordered_list.h"
#include "hotstuff/graph.h"

namespace hotstuff {

enum EntityType {
    ENT_TYPE_CMD = 0x0,
    ENT_TYPE_BLK = 0x1
};

struct ReplicaInfo {
    ReplicaID id;
    salticidae::PeerId peer_id;
    pubkey_bt pubkey;

    ReplicaInfo(ReplicaID id,
                const salticidae::PeerId &peer_id,
                pubkey_bt &&pubkey):
        id(id), peer_id(peer_id), pubkey(std::move(pubkey)) {}

    ReplicaInfo(const ReplicaInfo &other):
        id(other.id), peer_id(other.peer_id),
        pubkey(other.pubkey->clone()) {}

    ReplicaInfo(ReplicaInfo &&other):
        id(other.id), peer_id(other.peer_id),
        pubkey(std::move(other.pubkey)) {}
};

class ReplicaConfig {
    std::unordered_map<ReplicaID, ReplicaInfo> replica_map;

    public:
    size_t nreplicas;
    size_t nmajority;
    double fairness_parameter;      // Themis
    double solid_tx_threshold;      // Themis
    double non_blank_tx_threshold;  // Themis
    double tx_edge_threshold;       // Themis

    ReplicaConfig(): nreplicas(0), 
                        nmajority(0), 
                        fairness_parameter(1), 
                        solid_tx_threshold(0),
                        non_blank_tx_threshold(0), 
                        tx_edge_threshold(0) {}   // Themis

    void add_replica(ReplicaID rid, const ReplicaInfo &info) {
        replica_map.insert(std::make_pair(rid, info));
        nreplicas++;
    }

    const ReplicaInfo &get_info(ReplicaID rid) const {
        auto it = replica_map.find(rid);
        if (it == replica_map.end())
            throw HotStuffError("rid %s not found",
                    get_hex(rid).c_str());
        return it->second;
    }

    const PubKey &get_pubkey(ReplicaID rid) const {
        return *(get_info(rid).pubkey);
    }

    const salticidae::PeerId &get_peer_id(ReplicaID rid) const {
        return get_info(rid).peer_id;
    }
};

class Block;
class HotStuffCore;

using block_t = salticidae::ArcObj<Block>;

class Command: public Serializable {
    friend HotStuffCore;
    public:
    virtual ~Command() = default;
    virtual const uint256_t &get_hash() const = 0;
    virtual bool verify() const = 0;
    virtual operator std::string () const {
        DataStream s;
        s << "<cmd id=" << get_hex10(get_hash()) << ">";
        return s;
    }
};

using command_t = ArcObj<Command>;

template<typename Hashable>
inline static std::vector<uint256_t>
get_hashes(const std::vector<Hashable> &plist) {
    std::vector<uint256_t> hashes;
    for (const auto &p: plist)
        hashes.push_back(p->get_hash());
    return hashes;
}

class Block {
    friend HotStuffCore;
    std::vector<uint256_t> parent_hashes;
    // std::vector<uint256_t> cmds;                                         // Themis
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;     // Themis
    std::vector<std::pair<uint256_t, uint256_t>> e_missing;                 // Rashnu
    std::vector<std::pair<uint256_t, uint256_t>> e_update;                  // Themis
    quorum_cert_bt qc;
    bytearray_t extra;

    /* the following fields can be derived from above */
    uint256_t hash;
    std::vector<block_t> parents;
    block_t qc_ref;
    quorum_cert_bt self_qc;
    uint32_t height;
    bool delivered;
    int8_t decision;

    std::unordered_set<ReplicaID> voted;

    public:
    Block():
        qc(nullptr),
        qc_ref(nullptr),
        self_qc(nullptr), height(0),
        delivered(false), decision(0) {}

    Block(bool delivered, int8_t decision):
        qc(new QuorumCertDummy()),
        hash(salticidae::get_hash(*this)),
        qc_ref(nullptr),
        self_qc(nullptr), height(0),
        delivered(delivered), decision(decision) {}

    Block(const std::vector<block_t> &parents,
        // const std::vector<uint256_t> &cmds,                                  // Themis
        std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph,     // Themis
        std::vector<std::pair<uint256_t, uint256_t>> e_missing,
        std::vector<std::pair<uint256_t, uint256_t>> e_update,                  // Themis
        quorum_cert_bt &&qc,
        bytearray_t &&extra,
        uint32_t height,
        const block_t &qc_ref,
        quorum_cert_bt &&self_qc,
        int8_t decision = 0):
            parent_hashes(get_hashes(parents)),
            // cmds(cmds),          // Themis
            graph(graph),           // Themis
            e_missing(e_missing),   // Rashnu
            e_update(e_update),     // Themis
            qc(std::move(qc)),
            extra(std::move(extra)),
            hash(salticidae::get_hash(*this)),
            parents(parents),
            qc_ref(qc_ref),
            self_qc(std::move(self_qc)),
            height(height),
            delivered(0),
            decision(decision) {}

    void serialize(DataStream &s) const;

    void unserialize(DataStream &s, HotStuffCore *hsc);

    // Themis
    // const std::vector<uint256_t> &get_cmds() const {
    //     return cmds;
    // }

    // Themis
    const std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &get_graph() const {
        return graph;
    }

    // Themis
    void update_graph(std::pair<uint256_t, uint256_t> const &edge){
        if(graph.count(edge.first)>0 && graph.count(edge.second)>0){
            graph[edge.first].insert(edge.second);
            /** remove missing edge from e_missing **/
            for(int i=0; i<e_missing.size(); i++){
                auto missing_edge = e_missing[i];
                if((missing_edge.first==edge.first && missing_edge.second==edge.second)
                    || (missing_edge.first==edge.second && missing_edge.second==edge.first)){
                        e_missing.erase(e_missing.begin()+i);
                        i--;
                        break;
                }
            }
        }
    }

    // Themis
    void remove_cmd(uint256_t cmd_hash){
        graph.erase(cmd_hash);
        for(auto &g: graph){
            g.second.erase(cmd_hash);
        }
    }

    // Themis
    std::vector<std::pair<uint256_t, uint256_t>> get_missing_edges() {
        // std::vector<std::pair<uint256_t, uint256_t>> missing_edges;
        // /* Get all the nodes */
        // std::vector<uint256_t> nodes;
        // for (auto const &g: graph) {
        //     nodes.push_back(g.first);
        // }

        // /* Check if there is exactly one edge between any 2 pair of vertices */
        // size_t n = nodes.size();
        // uint256_t node_1, node_2;
        // for (size_t i=0; i<n; i++) {
        //     node_1 = nodes[i];
        //     for (size_t j=i+1; j<n; j++) {
        //         node_2 = nodes[j];
        //         if(graph[node_1].count(node_2)==0 && graph[node_2].count(node_1)==0) {
        //             // no edge found between these two nodes
        //             missing_edges.push_back(std::make_pair(node_1, node_2));
        //         }
        //     }
        // }

        // return missing_edges;
        return e_missing;
    }

    // Rashnu
    bool is_weakly_connected(){
        return e_missing.size()==0;
    }

    // Themis
    const std::vector<std::pair<uint256_t, uint256_t>> &get_e_update() const {
        return e_update;
    }

    const std::vector<block_t> &get_parents() const {
        return parents;
    }

    const std::vector<uint256_t> &get_parent_hashes() const {
        return parent_hashes;
    }

    const uint256_t &get_hash() const { return hash; }

    bool verify(const HotStuffCore *hsc) const;

    promise_t verify(const HotStuffCore *hsc, VeriPool &vpool) const;

    int8_t get_decision() const { return decision; }

    bool is_delivered() const { return delivered; }

    uint32_t get_height() const { return height; }

    const quorum_cert_bt &get_qc() const { return qc; }

    const block_t &get_qc_ref() const { return qc_ref; }

    const bytearray_t &get_extra() const { return extra; }

    operator std::string () const {
        DataStream s;
        s << "<block "
          << "id="  << get_hex10(hash) << " "
          << "height=" << std::to_string(height) << " "
          << "parent=" << get_hex10(parent_hashes[0]) << " "
          << "qc_ref=" << (qc_ref ? get_hex10(qc_ref->get_hash()) : "null") << ">";
        return s;
    }
};

struct BlockHeightCmp {
    bool operator()(const block_t &a, const block_t &b) const {
        return a->get_height() < b->get_height();
    }
};

class EntityStorage {
    std::mutex mtx_local_order_seen_execute_level_cache;
    std::mutex mtx_local_order_seen_propose_level_cache;
    std::mutex mtx_dependency_cache;
    std::mutex mtx_proposed_cmds_cache;
    std::unordered_map<const uint256_t, block_t> blk_cache;
    std::unordered_map<const uint256_t, command_t> cmd_cache;
    std::unordered_map<ReplicaID, std::deque<std::unordered_map<uint256_t, std::unordered_set<uint256_t>>>> ordered_dag_cache;
    std::unordered_map<ReplicaID, std::queue<std::vector<std::pair<uint256_t, uint256_t>>>> l_update_cache;   // Themis
    OrderedList *local_order_seen_execute_level_cache;                                            // Themis
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> edges_missing_cache;             // Themis
    OrderedList *local_order_seen_propose_level_cache;                                            // Themis
    std::unordered_set<uint256_t> proposed_cmds_cache; 
	std::unordered_map<const uint256_t, std::unordered_map<uint64_t, char>> dependency_cache;     // Rashnu                                           // Themis 

    public:
    EntityStorage() {
        local_order_seen_execute_level_cache = new OrderedList();
        local_order_seen_propose_level_cache = new OrderedList();
    }

    bool is_blk_delivered(const uint256_t &blk_hash) {
        auto it = blk_cache.find(blk_hash);
        if (it == blk_cache.end()) return false;
        return it->second->is_delivered();
    }

    bool is_blk_fetched(const uint256_t &blk_hash) {
        return blk_cache.count(blk_hash);
    }

    block_t add_blk(Block &&_blk, const ReplicaConfig &/*config*/) {
        //if (!_blk.verify(config))
        //{
        //    HOTSTUFF_LOG_WARN("invalid %s", std::string(_blk).c_str());
        //    return nullptr;
        //}
        block_t blk = new Block(std::move(_blk));
        return blk_cache.insert(std::make_pair(blk->get_hash(), blk)).first->second;
    }

    const block_t &add_blk(const block_t &blk) {
        return blk_cache.insert(std::make_pair(blk->get_hash(), blk)).first->second;
    }

    block_t find_blk(const uint256_t &blk_hash) {
        auto it = blk_cache.find(blk_hash);
        return it == blk_cache.end() ? nullptr : it->second;
    }

    bool is_cmd_fetched(const uint256_t &cmd_hash) {
        return cmd_cache.count(cmd_hash);
    }

    const command_t &add_cmd(const command_t &cmd) {
        return cmd_cache.insert(std::make_pair(cmd->get_hash(), cmd)).first->second;
    }

    command_t find_cmd(const uint256_t &cmd_hash) {
        auto it = cmd_cache.find(cmd_hash);
        return it == cmd_cache.end() ? nullptr: it->second;
    }

    size_t get_cmd_cache_size() {
        return cmd_cache.size();
    }
    size_t get_blk_cache_size() {
        return blk_cache.size();
    }

    bool try_release_cmd(const command_t &cmd) {
        if (cmd.get_cnt() == 2) /* only referred by cmd and the storage */
        {
            const auto &cmd_hash = cmd->get_hash();
            cmd_cache.erase(cmd_hash);
            return true;
        }
        return false;
    }

    bool try_release_blk(const block_t &blk) {
        if (blk.get_cnt() == 2) /* only referred by blk and the storage */
        {
            const auto &blk_hash = blk->get_hash();
#ifdef HOTSTUFF_PROTO_LOG
            HOTSTUFF_LOG_INFO("releasing blk %.10s", get_hex(blk_hash).c_str());
#endif
//            for (const auto &cmd: blk->get_cmds())
//                try_release_cmd(cmd);
            blk_cache.erase(blk_hash);
            return true;
        }
#ifdef HOTSTUFF_PROTO_LOG
        else
            HOTSTUFF_LOG_INFO("cannot release (%lu)", blk.get_cnt());
#endif
        return false;
    }

    // Themis
    void add_local_order(ReplicaID rid, std::unordered_map<uint256_t, std::unordered_set<uint256_t>> ordered_dag, 
                            const std::vector<std::pair<uint256_t, uint256_t>> l_update){
        /* Overwriting old values if exists */
        std::unordered_map<uint256_t, std::unordered_set<uint256_t>> unproposed_ordered_dag;
        for(auto &g: ordered_dag){
            auto cmd = g.first;
            if(!is_cmd_proposed(cmd)){
                unproposed_ordered_dag[cmd] = std::unordered_set<uint256_t>();
            }
        }
        
        if(!unproposed_ordered_dag.empty()){
            if(unproposed_ordered_dag.size()<ordered_dag.size()){
                /* remove the proposed cmds from ordered dag and adjust the graph */
                /* ordered_dag = A->B->C, B is already proposed then */
                /* unproposed_ordered_dag = A->C */
                for(auto &g: ordered_dag){
                    auto from = g.first;
                    if(unproposed_ordered_dag.count(from)==0){
                        continue;
                    }
                    for(auto to: g.second){
                        if(unproposed_ordered_dag.count(to)==0){
                            /* Remove the proposed hashes (eg remove B) */
                            unproposed_ordered_dag[from].insert(ordered_dag[to].begin(), ordered_dag[to].end());
                        }
                        else{
                            /* Keep the unproposed hash linking */
                            unproposed_ordered_dag[from].insert(to);
                        }
                    }
                }
            }
            else{
                unproposed_ordered_dag = ordered_dag;
            }

            /* update the cache */
            ordered_dag_cache[rid].push_back(unproposed_ordered_dag);
        }

        l_update_cache[rid].push(l_update);
    }

      // Rashnu
    void add_ordered_dag_to_front(ReplicaID rid, std::unordered_map<uint256_t, std::unordered_set<uint256_t>> ordered_dag){
        ordered_dag_cache[rid].push_front(ordered_dag);
    }

    // Themis
    void clear_front_ordered_dag(ReplicaID replica) {
        ordered_dag_cache[replica].pop_front();
        if(ordered_dag_cache[replica].empty()){
            ordered_dag_cache.erase(replica);
        }
    }
    // Themis
    void clear_front_l_update(ReplicaID replica) {
        l_update_cache[replica].pop();
        if(l_update_cache[replica].empty()){
            l_update_cache.erase(replica);
        }
    }

    // Themis
    size_t get_local_order_cache_size(){
        return ordered_dag_cache.size();
    }

    // Themis
    std::vector<ReplicaID> get_ordered_dag_replia_vector(){
        std::vector<ReplicaID> replicas;
        for(auto const& order: ordered_dag_cache){
            replicas.push_back(order.first);
        }
        return replicas;
    }

    // Themis
    std::vector<ReplicaID> get_l_update_replia_vector(){
        std::vector<ReplicaID> replicas;
        for(auto const& order: l_update_cache){
            replicas.push_back(order.first);
        }
        return replicas;
    }

    // Themis
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> get_ordered_dag(ReplicaID replica) {
        return ordered_dag_cache[replica].front();
    }

    // Themis
    std::vector<std::pair<uint256_t, uint256_t>> get_l_update_vector(ReplicaID replica) {
        return l_update_cache[replica].front();
    }

    // Themis
    void update_local_order_seen(std::vector<uint256_t> const &cmds) {
        for(auto const &cmd: cmds){
            update_local_order_seen(cmd);
        }
    }
    // Themis
    void update_local_order_seen(uint256_t const &cmd) {
        {
            std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_execute_level_cache);
            local_order_seen_execute_level_cache->push_back(cmd);
        }
        {
            std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_propose_level_cache);
            local_order_seen_propose_level_cache->push_back(cmd);
        }     
    }

    // Themis
    void remove_local_order_seen_execute_level(uint256_t cmd) {
        std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_execute_level_cache);
        local_order_seen_execute_level_cache->remove(cmd);
    }
    // Themis
    void remove_local_order_seen_propose_level(uint256_t cmd) {
        std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_propose_level_cache);
        local_order_seen_propose_level_cache->remove(cmd);
    }

    // Themis
    std::vector<uint256_t> get_unproposed_cmds() {
        // std::vector<uint256_t> cmds;
        // for(auto it=local_order_seen_propose_level_cache->begin(); it!=local_order_seen_propose_level_cache->end(); it++) {
        //     cmds.push_back(*it);
        // }
        // return cmds;
        std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_propose_level_cache);
        return local_order_seen_propose_level_cache->get_cmds();
    }


    // Themis
    std::vector<std::pair<uint256_t, uint256_t>> get_updated_missing_edges() {
        
// #ifdef HOTSTUFF_ENABLE_LOG_DEBUG
#ifdef NOTDEFINE
        HOTSTUFF_LOG_DEBUG("[[get_updated_missing_edges]] [R-] [L-] edges_missing_cache size = %d", edges_missing_cache.size());

        for(auto const &cache: edges_missing_cache){
            auto const from_v = cache.first;
            for(auto const to_v: cache.second){
                HOTSTUFF_LOG_DEBUG("[[get_updated_missing_edges]] [R-] [L-] edges_missing_cache edge = %.10s -> %.10s", get_hex(from_v).c_str(), get_hex(to_v).c_str());
            }
        }
#endif

        // std::vector<std::pair<uint256_t, uint256_t>> edges;
        // for(auto it_1=local_order_seen_execute_level_cache->begin(); it_1!=local_order_seen_execute_level_cache->end(); it_1++) {
        //     auto const from_v = *it_1;
        //     for(auto it_2=it_1.next(); it_2!=local_order_seen_execute_level_cache->end(); it_2++) {
        //         auto const to_v = *it_2;
        //         HOTSTUFF_LOG_INFO("[[get_updated_missing_edges]] [R-] [L-] local_order_seen_cache edge = %.10s -> %.10s", get_hex(from_v).c_str(), get_hex(to_v).c_str());
        //         if(edges_missing_cache[from_v].count(to_v)>0 || edges_missing_cache[to_v].count(from_v)>0) {
        //             HOTSTUFF_LOG_INFO("[[get_updated_missing_edges]] current missing = (%.10s, %.10s)", from_v, to_v);
        //             edges.push_back(std::make_pair(from_v, to_v));
        //         }
        //     }
        // }
        // return edges;
        std::unique_lock<std::mutex> lock(this->mtx_local_order_seen_execute_level_cache);
        return local_order_seen_execute_level_cache->get_curr_missing_edges(edges_missing_cache);
    }

    // Themis
    void add_missing_edge(uint256_t v1, uint256_t v2) {
        if(edges_missing_cache[v2].count(v1)>0){
            return;
        }
        edges_missing_cache[v1].insert(v2);
    }   

    // Themis
    void remove_missing_edge(uint256_t v1, uint256_t v2) {
        if(edges_missing_cache[v1].count(v2)>0){
            edges_missing_cache[v1].erase(v2);
        }
        if(edges_missing_cache[v2].count(v1)>0){
            edges_missing_cache[v2].erase(v1);
        }
    }   

    // Themis Dummy
    void add_to_proposed_cmds_cache(uint256_t cmd){
        std::unique_lock<std::mutex> lock(this->mtx_proposed_cmds_cache);
        proposed_cmds_cache.insert(cmd);
    }   

    // Themis Dummy
    void remove_from_proposed_cmds_cache(uint256_t cmd){
        std::unique_lock<std::mutex> lock(this->mtx_proposed_cmds_cache);
        proposed_cmds_cache.erase(cmd);
    }  

    // Themis Dummy
    bool is_cmd_proposed(uint256_t cmd){
        std::unique_lock<std::mutex> lock(this->mtx_proposed_cmds_cache);
        return proposed_cmds_cache.count(cmd)>0;
    }  
	
	// Rashnu
    void update_dependency_cache(uint256_t cmd_hash, std::unordered_map<uint64_t, char> dependency){
        std::unique_lock<std::mutex> lock(this->mtx_dependency_cache);
        dependency_cache[cmd_hash] = dependency;
    }  

    // Rashnu
    std::unordered_map<uint64_t, char> get_cmd_dependency(uint256_t cmd_hash){
        std::unique_lock<std::mutex> lock(this->mtx_dependency_cache);
        if(dependency_cache.count(cmd_hash)>0){
            return dependency_cache[cmd_hash];
        }
        return std::unordered_map<uint64_t, char>();
    }      

    // Rashnu
    void erase_cmd_dependency(uint256_t cmd_hash){
        std::unique_lock<std::mutex> lock(this->mtx_dependency_cache);
         if(dependency_cache.count(cmd_hash)>0){
            dependency_cache.erase(cmd_hash);
        }
    }    

};

}

#endif
