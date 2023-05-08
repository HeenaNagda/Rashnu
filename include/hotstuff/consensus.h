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

#ifndef _HOTSTUFF_CONSENSUS_H
#define _HOTSTUFF_CONSENSUS_H

#include <cassert>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <deque>

#include "hotstuff/promise.hpp"
#include "hotstuff/type.h"
#include "hotstuff/entity.h"
#include "hotstuff/crypto.h"
#include "hotstuff/graph.h"

namespace hotstuff {

struct Proposal;
struct Vote;
struct LocalOrder;
struct Finality;

template <typename T>
class BlockingQueue{
private:
    std::deque<T> d_queue;
    std::condition_variable cond;
    std::mutex mtx;
    bool terminate;
public:
    BlockingQueue();
    void push(T const& value);
    T pop();
    bool empty();
    void do_terminate();
    bool termination_state();
};
class ThreadPool{
private:
    hotstuff::ReplicaID rid; 
    std::atomic_int cmd_id;
    uint32_t cmd_height;
    salticidae::uint256_t blk_hash;
    HotStuffCore *hsc;

    std::unordered_map<salticidae::uint256_t, std::unordered_set<salticidae::uint256_t>> graph;
    std::unordered_map<salticidae::uint256_t, size_t> n_incoming;
    size_t leaf_count;
    std::mutex mtx_n_incoming;
    std::mutex mtx_leaf_count;
    std::mutex mtx_storage;
    std::vector<std::thread*> threads;
    size_t pool_size;
    BlockingQueue<uint256_t> shared_queue;

    void execute();
    int get_cmd_id();

public:
    ThreadPool(size_t pool_size, std::unordered_map<salticidae::uint256_t, std::unordered_set<salticidae::uint256_t>> &graph, hotstuff::ReplicaID rid, uint32_t cmd_height, salticidae::uint256_t blk_hash, HotStuffCore *obj);
    void start_execute_parallel();
};

/** Abstraction for HotStuff protocol state machine (without network implementation). */
class HotStuffCore {
    block_t b0;                                  /** the genesis block */
    /* === state variables === */
    /** block containing the QC for the highest block having one */
    std::pair<block_t, quorum_cert_bt> hqc;   /**< highest QC */
    block_t b_lock;                            /**< locked block */
    block_t b_exec;                            /**< last executed block */
    uint32_t vheight;          /**< height of the block last voted for */
    /* === auxilliary variables === */
    privkey_bt priv_key;            /**< private key for signing votes */
    std::set<block_t> tails;   /**< set of tail blocks */
    ReplicaConfig config;                   /**< replica configuration */
    /* === async event queues === */
    std::unordered_map<block_t, promise_t> qc_waiting;
    promise_t propose_waiting;
    promise_t receive_proposal_waiting;
    promise_t hqc_update_waiting;
    /* == feature switches == */
    /** always vote negatively, useful for some PaceMakers */
    bool vote_disabled;

    block_t get_delivered_blk(const uint256_t &blk_hash);
    void sanity_check_delivered(const block_t &blk);
    void update(const block_t &nblk);
    void update_hqc(const block_t &_hqc, const quorum_cert_bt &qc);
    void on_hqc_update();
    void on_qc_finish(const block_t &blk);
    void on_propose_(const Proposal &prop);
    void on_receive_proposal_(const Proposal &prop);

    protected:
    ReplicaID id;                  /**< identity of the replica itself */

    public:
    BoxObj<EntityStorage> storage;

    HotStuffCore(ReplicaID id, privkey_bt &&priv_key);
    virtual ~HotStuffCore() {
        b0->qc_ref = nullptr;
    }

    /* Inputs of the state machine triggered by external events, should called
     * by the class user, with proper invariants. */

    /** Call to initialize the protocol, should be called once before all other
     * functions. */
    void on_init(uint32_t nfaulty, double fairness_parameter);  // Themis

    /* TODO: better name for "delivery" ? */
    /** Call to inform the state machine that a block is ready to be handled.
     * A block is only delivered if itself is fetched, the block for the
     * contained qc is fetched and all parents are delivered. The user should
     * always ensure this invariant. The invalid blocks will be dropped by this
     * function.
     * @return true if valid */
    bool on_deliver_blk(const block_t &blk);

    /** Call upon the delivery of a proposal message.
     * The block mentioned in the message should be already delivered. */
    void on_receive_proposal(const Proposal &prop);

    /** Call upon the delivery of a vote message.
     * The block mentioned in the message should be already delivered. */
    void on_receive_vote(const Vote &vote);

    /** Call to submit new commands to be decided (executed). "Parents" must
     * contain at least one block, and the first block is the actual parent,
     * while the others are uncles/aunts */
    block_t on_propose(/* const std::vector<uint256_t> &cmds,*/                 // Themis
                    const std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &graph,
                    const std::vector<std::pair<uint256_t, uint256_t>> &e_missing,  // Rashnu
                    const std::vector<std::pair<uint256_t, uint256_t>> &e_update,
                    const std::vector<block_t> &parents,
                    bytearray_t &&extra = bytearray_t());

    void print_block(std::string calling_method,const hotstuff::Proposal &prop);                       // Themis
    /** Call to submit local order to the current leader **/
    void on_local_order (ReplicaID proposer, const std::vector<uint256_t> &order, bool is_reorder=false);       // Themis
    /** Called when local order is received on Leader from a Replica  **/
    bool on_receive_local_order (const LocalOrder &local_order, const std::vector<block_t> &parents);   // Themis
    /** Print() **/
    void print_all_blocks(const block_t &nblk, const block_t &blk);     // Themis
    /** FairFinalize() **/
    std::vector<uint256_t> fair_finalize_old(block_t const &blk, std::vector<std::pair<uint256_t, uint256_t>> const &e_update);       // Themis
    std::unordered_map<salticidae::uint256_t, std::unordered_set<salticidae::uint256_t>> fair_finalize(block_t const &blk, std::vector<std::pair<uint256_t, uint256_t>> const &e_update); // Rashnu
    /** FairPropose() **/
    std::pair<std::unordered_map<uint256_t, std::unordered_set<uint256_t>>, std::vector<std::pair<uint256_t, uint256_t>>> fair_propose();        // Rashnus
    std::vector<std::pair<uint256_t, uint256_t>> fair_update();                         // Themis
    void reorder(ReplicaID proposer);                                                                   // Themis

    /** Themis thereshold Initialization **/
    double get_solid_tx_threshold();    // Themis
    double get_non_blank_tx_threshold();// Themis
    double get_tx_edge_threshold();     // Themis

    /* Functions required to construct concrete instances for abstract classes.
     * */

    /* Outputs of the state machine triggering external events.  The virtual
     * functions should be implemented by the user to specify the behavior upon
     * the events. */

    /** Called by HotStuffCore upon the decision being made for cmd. */
    virtual void do_decide(Finality &&fin) = 0;
    
    protected:

    virtual void do_consensus(const block_t &blk) = 0;
    /** Called by HotStuffCore upon broadcasting a new proposal.
     * The user should send the proposal message to all replicas except for
     * itself. */
    virtual void do_broadcast_proposal(const Proposal &prop) = 0;
    /** Called upon sending out a new vote to the next proposer.  The user
     * should send the vote message to a *good* proposer to have good liveness,
     * while safety is always guaranteed by HotStuffCore. */
    virtual void do_vote(ReplicaID last_proposer, const Vote &vote) = 0;
    /** Called upon sending out local ordering to the next proposer. */
    virtual void do_send_local_order(ReplicaID proposer, const LocalOrder &local_order) = 0;        // Themis

    /* The user plugs in the detailed instances for those
     * polymorphic data types. */
    public:
    /** Create a partial certificate that proves the vote for a block. */
    virtual part_cert_bt create_part_cert(const PrivKey &priv_key, const uint256_t &blk_hash) = 0;
    /** Create a partial certificate from its seralized form. */
    virtual part_cert_bt parse_part_cert(DataStream &s) = 0;
    /** Create a quorum certificate that proves 2f+1 votes for a block. */
    virtual quorum_cert_bt create_quorum_cert(const uint256_t &blk_hash) = 0;
    /** Create a quorum certificate from its serialized form. */
    virtual quorum_cert_bt parse_quorum_cert(DataStream &s) = 0;
    /** Create a command object from its serialized form. */
    //virtual command_t parse_cmd(DataStream &s) = 0;

    public:
    /** Add a replica to the current configuration. This should only be called
     * before running HotStuffCore protocol. */
    void add_replica(ReplicaID rid, const PeerId &peer_id, pubkey_bt &&pub_key);
    /** Try to prune blocks lower than last committed height - staleness. */
    void prune(uint32_t staleness);

    /* PaceMaker can use these functions to monitor the core protocol state
     * transition */
    /** Get a promise resolved when the block gets a QC. */
    promise_t async_qc_finish(const block_t &blk);
    /** Get a promise resolved when a new block is proposed. */
    promise_t async_wait_proposal();
    /** Get a promise resolved when a new proposal is received. */
    promise_t async_wait_receive_proposal();
    /** Get a promise resolved when hqc is updated. */
    promise_t async_hqc_update();

    /* Other useful functions */
    const block_t &get_genesis() const { return b0; }
    const block_t &get_hqc() { return hqc.first; }
    const ReplicaConfig &get_config() const { return config; }
    ReplicaID get_id() const { return id; }
    const std::set<block_t> get_tails() const { return tails; }
    operator std::string () const;
    void set_vote_disabled(bool f) { vote_disabled = f; }
};

/** Abstraction for proposal messages. */
struct Proposal: public Serializable {
    ReplicaID proposer;
    /** block being proposed */
    block_t blk;
    /** handle of the core object to allow polymorphism. The user should use
     * a pointer to the object of the class derived from HotStuffCore */
    HotStuffCore *hsc;

    Proposal(): blk(nullptr), hsc(nullptr) {}
    Proposal(ReplicaID proposer,
            const block_t &blk,
            HotStuffCore *hsc):
        proposer(proposer),
        blk(blk), hsc(hsc) {}

    void serialize(DataStream &s) const override {
        s << proposer
          << *blk;
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);
        s >> proposer;
        Block _blk;
        _blk.unserialize(s, hsc);
        blk = hsc->storage->add_blk(std::move(_blk), hsc->get_config());
    }

    operator std::string () const {
        DataStream s;
        s << "<proposal "
          << "rid=" << std::to_string(proposer) << " "
          << "blk=" << get_hex10(blk->get_hash()) << ">";
        return s;
    }
};

/** Abstraction for vote messages. */
struct Vote: public Serializable {
    ReplicaID voter;
    /** block being voted */
    uint256_t blk_hash;
    /** proof of validity for the vote */
    part_cert_bt cert;
    
    /** handle of the core object to allow polymorphism */
    HotStuffCore *hsc;

    Vote(): cert(nullptr), hsc(nullptr) {}
    Vote(ReplicaID voter,
        const uint256_t &blk_hash,
        part_cert_bt &&cert,
        HotStuffCore *hsc):
        voter(voter),
        blk_hash(blk_hash),
        cert(std::move(cert)), hsc(hsc) {}

    Vote(const Vote &other):
        voter(other.voter),
        blk_hash(other.blk_hash),
        cert(other.cert ? other.cert->clone() : nullptr),
        hsc(other.hsc) {}

    Vote(Vote &&other) = default;
    
    void serialize(DataStream &s) const override {
        s << voter << blk_hash << *cert;
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);
        s >> voter >> blk_hash;
        cert = hsc->parse_part_cert(s);
    }

    bool verify() const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                cert->get_obj_hash() == blk_hash;
    }

    promise_t verify(VeriPool &vpool) const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
            return result && cert->get_obj_hash() == blk_hash;
        });
    }

    operator std::string () const {
        DataStream s;
        s << "<vote "
          << "rid=" << std::to_string(voter) << " "
          << "blk=" << get_hex10(blk_hash) << ">";
        return s;
    }
};

// Themis
/** Abstraction for LocalOrder messages. */
struct LocalOrder: public Serializable {
    ReplicaID initiator;
    /** Local ordering as seen by replica "initiator" **/
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> ordered_dag;
    /** Local transaction ordering for previously proposed shaded transaction and have missing edges **/
    std::vector<std::pair<uint256_t, uint256_t>> l_update;
    /** handle of the core object to allow polymorphism */
    HotStuffCore *hsc;

    LocalOrder(): hsc(nullptr) {}
    LocalOrder(ReplicaID initiator, 
                const std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &ordered_dag, 
                const std::vector<std::pair<uint256_t, uint256_t>> &l_update,
                HotStuffCore *hsc) : 
                    initiator(initiator),
                    ordered_dag(ordered_dag),
                    l_update(l_update),
                    hsc(hsc){}

    LocalOrder(const LocalOrder &other):
                initiator(other.initiator),
                ordered_dag(other.ordered_dag),
                l_update(other.l_update),
                hsc(other.hsc){}

    LocalOrder(LocalOrder &&other) = default;

    void serialize(DataStream &s) const override {
        /** Serilize replica ID **/
        s << initiator;

        /** Serialize local ordering transaction hashes **/
        GraphFormatConversion *format = new GraphFormatConversion();
        format->serialize(s, ordered_dag);

        /** Serialize edges that were missing in previous proposals and found now on the replica **/
        s << htole((uint32_t)l_update.size());
        for (const auto &update: l_update) {
            s << update.first;
            s << update.second;
        }
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);

        /** unserialize the replica id **/
        s >> initiator;

        /** unserialized ordered_hashes **/
        GraphFormatConversion *format = new GraphFormatConversion();
        format->unserialize(s, ordered_dag);

        /** unserialized l_update **/
        uint32_t size;
        s >> size;
        size = letoh(size);
        l_update.resize(size);
        for (auto &edge: l_update)
        {
            s >> edge.first;
            s >> edge.second;
        }
    }

    operator std::string () const {
        DataStream s;
        s << "<LocalOrder "
          << "rid=" << std::to_string(initiator) << " "
          << "orderedDAG=";
        
        for(auto g: ordered_dag){
            s << "{ " << g.first << ":";
            for(auto cmd: g.second){
                s << cmd;
            }
            s << " }";
        }
        s << " L_update=";
        for (auto &edge: l_update)
        {
            s << "(" << edge.first << "," << edge.second << "),";
        }
        s << ">";
        return s;  
    }
};

struct Finality: public Serializable {
    ReplicaID rid;
    int8_t decision;
    uint32_t cmd_idx;
    uint32_t cmd_height;
    uint256_t cmd_hash;
    uint256_t blk_hash;
    
    public:
    Finality() = default;
    Finality(ReplicaID rid,
            int8_t decision,
            uint32_t cmd_idx,
            uint32_t cmd_height,
            uint256_t cmd_hash,
            uint256_t blk_hash):
        rid(rid), decision(decision),
        cmd_idx(cmd_idx), cmd_height(cmd_height),
        cmd_hash(cmd_hash), blk_hash(blk_hash) {}

    void serialize(DataStream &s) const override {
        s << rid << decision
          << cmd_idx << cmd_height
          << cmd_hash;
        if (decision == 1) s << blk_hash;
    }

    void unserialize(DataStream &s) override {
        s >> rid >> decision
          >> cmd_idx >> cmd_height
          >> cmd_hash;
        if (decision == 1) s >> blk_hash;
    }

    operator std::string () const {
        DataStream s;
        s << "<fin "
          << "decision=" << std::to_string(decision) << " "
          << "cmd_idx=" << std::to_string(cmd_idx) << " "
          << "cmd_height=" << std::to_string(cmd_height) << " "
          << "cmd=" << get_hex10(cmd_hash) << " "
          << "blk=" << get_hex10(blk_hash) << ">";
        return s;
    }
};

}

#endif
