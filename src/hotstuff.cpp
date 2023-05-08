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

#include "hotstuff/hotstuff.h"
#include "hotstuff/client.h"
#include "hotstuff/liveness.h"

using salticidae::static_pointer_cast;

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN

namespace hotstuff {

const opcode_t MsgPropose::opcode;
MsgPropose::MsgPropose(const Proposal &proposal) { serialized << proposal; }
void MsgPropose::postponed_parse(HotStuffCore *hsc) {
    proposal.hsc = hsc;
    serialized >> proposal;
}

const opcode_t MsgVote::opcode;
MsgVote::MsgVote(const Vote &vote) { serialized << vote; }
void MsgVote::postponed_parse(HotStuffCore *hsc) {
    vote.hsc = hsc;
    serialized >> vote;
}

const opcode_t MsgReqBlock::opcode;
MsgReqBlock::MsgReqBlock(const std::vector<uint256_t> &blk_hashes) {
    serialized << htole((uint32_t)blk_hashes.size());
    for (const auto &h: blk_hashes)
        serialized << h;
}

MsgReqBlock::MsgReqBlock(DataStream &&s) {
    uint32_t size;
    s >> size;
    size = letoh(size);
    blk_hashes.resize(size);
    for (auto &h: blk_hashes) s >> h;
}

const opcode_t MsgRespBlock::opcode;
MsgRespBlock::MsgRespBlock(const std::vector<block_t> &blks) {
    serialized << htole((uint32_t)blks.size());
    for (auto blk: blks) serialized << *blk;
}

void MsgRespBlock::postponed_parse(HotStuffCore *hsc) {
    uint32_t size;
    serialized >> size;
    size = letoh(size);
    blks.resize(size);
    for (auto &blk: blks)
    {
        Block _blk;
        _blk.unserialize(serialized, hsc);
        blk = hsc->storage->add_blk(std::move(_blk), hsc->get_config());
    }
}


// Themis
const opcode_t MsgLocalOrder::opcode;
MsgLocalOrder::MsgLocalOrder(const LocalOrder &local_order) { serialized << local_order; }
void MsgLocalOrder::postponed_parse(HotStuffCore *hsc) {
    local_order.hsc = hsc;
    serialized >> local_order;
}


// TODO: improve this function
void HotStuffBase::exec_command(uint256_t cmd_hash, std::unordered_map<uint64_t, char> dependency, commit_cb_t callback) {
    storage->update_dependency_cache(cmd_hash, dependency);
    cmd_pending.enqueue(std::make_pair(cmd_hash, callback));
}

void HotStuffBase::on_fetch_blk(const block_t &blk) {
#ifdef HOTSTUFF_BLK_PROFILE
    blk_profiler.get_tx(blk->get_hash());
#endif
    LOG_DEBUG("fetched %.10s", get_hex(blk->get_hash()).c_str());
    part_fetched++;
    fetched++;
    //for (auto cmd: blk->get_cmds()) on_fetch_cmd(cmd);
    const uint256_t &blk_hash = blk->get_hash();
    auto it = blk_fetch_waiting.find(blk_hash);
    if (it != blk_fetch_waiting.end())
    {
        it->second.resolve(blk);
        blk_fetch_waiting.erase(it);
    }
}

bool HotStuffBase::on_deliver_blk(const block_t &blk) {
    const uint256_t &blk_hash = blk->get_hash();
    bool valid;
    /* sanity check: all parents must be delivered */
    for (const auto &p: blk->get_parent_hashes())
        assert(storage->is_blk_delivered(p));
    if ((valid = HotStuffCore::on_deliver_blk(blk)))
    {
        LOG_DEBUG("block %.10s delivered",
                get_hex(blk_hash).c_str());
        part_parent_size += blk->get_parent_hashes().size();
        part_delivered++;
        delivered++;
    }
    else
    {
        LOG_WARN("dropping invalid block");
    }

    bool res = true;
    auto it = blk_delivery_waiting.find(blk_hash);
    if (it != blk_delivery_waiting.end())
    {
        auto &pm = it->second;
        if (valid)
        {
            pm.elapsed.stop(false);
            auto sec = pm.elapsed.elapsed_sec;
            part_delivery_time += sec;
            part_delivery_time_min = std::min(part_delivery_time_min, sec);
            part_delivery_time_max = std::max(part_delivery_time_max, sec);

            pm.resolve(blk);
        }
        else
        {
            pm.reject(blk);
            res = false;
            // TODO: do we need to also free it from storage?
        }
        blk_delivery_waiting.erase(it);
    }
    return res;
}

promise_t HotStuffBase::async_fetch_blk(const uint256_t &blk_hash,
                                        const PeerId *replica,
                                        bool fetch_now) {
    if (storage->is_blk_fetched(blk_hash))
        return promise_t([this, &blk_hash](promise_t pm){
            pm.resolve(storage->find_blk(blk_hash));
        });
    auto it = blk_fetch_waiting.find(blk_hash);
    if (it == blk_fetch_waiting.end())
    {
#ifdef HOTSTUFF_BLK_PROFILE
        blk_profiler.rec_tx(blk_hash, false);
#endif
        it = blk_fetch_waiting.insert(
            std::make_pair(
                blk_hash,
                BlockFetchContext(blk_hash, this))).first;
    }
    if (replica != nullptr)
        it->second.add_replica(*replica, fetch_now);
    return static_cast<promise_t &>(it->second);
}

promise_t HotStuffBase::async_deliver_blk(const uint256_t &blk_hash,
                                        const PeerId &replica) {
    if (storage->is_blk_delivered(blk_hash))
        return promise_t([this, &blk_hash](promise_t pm) {
            pm.resolve(storage->find_blk(blk_hash));
        });
    auto it = blk_delivery_waiting.find(blk_hash);
    if (it != blk_delivery_waiting.end())
        return static_cast<promise_t &>(it->second);
    BlockDeliveryContext pm{[](promise_t){}};
    it = blk_delivery_waiting.insert(std::make_pair(blk_hash, pm)).first;
    /* otherwise the on_deliver_batch will resolve */
    async_fetch_blk(blk_hash, &replica).then([this, replica](block_t blk) {
        /* qc_ref should be fetched */
        std::vector<promise_t> pms;
        const auto &qc = blk->get_qc();
        assert(qc);
        if (blk == get_genesis())
            pms.push_back(promise_t([](promise_t &pm){ pm.resolve(true); }));
        else
            pms.push_back(blk->verify(this, vpool));
        pms.push_back(async_fetch_blk(qc->get_obj_hash(), &replica));
        /* the parents should be delivered */
        for (const auto &phash: blk->get_parent_hashes())
            pms.push_back(async_deliver_blk(phash, replica));
        promise::all(pms).then([this, blk](const promise::values_t values) {
            auto ret = promise::any_cast<bool>(values[0]) && this->on_deliver_blk(blk);
            if (!ret)
                HOTSTUFF_LOG_WARN("verification failed during async delivery");
        });
    });
    return static_cast<promise_t &>(pm);
}

void HotStuffBase::propose_handler(MsgPropose &&msg, const Net::conn_t &conn) {
    const PeerId &peer = conn->get_peer_id();
    if (peer.is_null()) return;
    msg.postponed_parse(this);
    auto &prop = msg.proposal;
    block_t blk = prop.blk;


    if (!blk) return;
    if (peer != get_config().get_peer_id(prop.proposer))
    {
        LOG_WARN("invalid proposal from %d", prop.proposer);
        return;
    }
    promise::all(std::vector<promise_t>{
        async_deliver_blk(blk->get_hash(), peer)
    }).then([this, prop = std::move(prop)]() {
        on_receive_proposal(prop);
    });
}

// void HotStuffBase::print_block(std::string calling_method, const hotstuff::Proposal &prop){
// #ifdef HOTSTUFF_ENABLE_LOG_DEBUG
//     for ( auto const &g: prop.blk->get_graph()) {
//         HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] key = %s", calling_method.c_str(), get_id(), prop.proposer, get_hex(g.first).c_str());
//         for (auto const &tx: g.second){
//             HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] val = %s", calling_method.c_str(), get_id(), prop.proposer, get_hex(tx).c_str());
//         }
//     }
// #endif

//     DataStream s1;
//     prop.blk->serialize(s1);
//     HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] block (serialized) = %s", calling_method.c_str(), get_id(), prop.proposer, s1.get_hex().c_str());
//     DataStream s2;
//     s2 << *prop.blk;
//     HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] block (salticidae raw) = %s", calling_method.c_str(), get_id(), prop.proposer, s2.get_hex().c_str());
//     HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] block (salticidae raw hash) = %s", calling_method.c_str(), get_id(), prop.proposer, get_hex(s2.get_hash()).c_str());
//     HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] block (salticidae hash) = %s", calling_method.c_str(), get_id(), prop.proposer, get_hex(salticidae::get_hash(*prop.blk)).c_str());
//     HOTSTUFF_LOG_DEBUG("[[%s]] [R-%d] [L-%d] broadcasted block = %s", calling_method.c_str(), get_id(), prop.proposer, get_hex(prop.blk->get_hash()).c_str());
// }

void HotStuffBase::vote_handler(MsgVote &&msg, const Net::conn_t &conn) {
    const auto &peer = conn->get_peer_id();
    if (peer.is_null()) return;
    msg.postponed_parse(this);
    //auto &vote = msg.vote;
    RcObj<Vote> v(new Vote(std::move(msg.vote)));
    promise::all(std::vector<promise_t>{
        async_deliver_blk(v->blk_hash, peer),
        v->verify(vpool),
    }).then([this, v=std::move(v)](const promise::values_t values) {
        if (!promise::any_cast<bool>(values[1]))
            LOG_WARN("invalid vote from %d", v->voter);
        else
            on_receive_vote(*v);
    });
}

void HotStuffBase::req_blk_handler(MsgReqBlock &&msg, const Net::conn_t &conn) {
    const PeerId replica = conn->get_peer_id();
    if (replica.is_null()) return;
    auto &blk_hashes = msg.blk_hashes;
    std::vector<promise_t> pms;
    for (const auto &h: blk_hashes)
        pms.push_back(async_fetch_blk(h, nullptr));
    promise::all(pms).then([replica, this](const promise::values_t values) {
        std::vector<block_t> blks;
        for (auto &v: values)
        {
            auto blk = promise::any_cast<block_t>(v);
            blks.push_back(blk);
        }
        pn.send_msg(MsgRespBlock(blks), replica);
    });
}

void HotStuffBase::resp_blk_handler(MsgRespBlock &&msg, const Net::conn_t &) {
    msg.postponed_parse(this);
    for (const auto &blk: msg.blks)
        if (blk) on_fetch_blk(blk);
}

// Themis
void HotStuffBase::local_order_handler(MsgLocalOrder &&msg, const Net::conn_t &conn) {
    const PeerId &peer = conn->get_peer_id();
    if (peer.is_null()) return;
    msg.postponed_parse(this);
    auto &local_order = msg.local_order;
    if (peer != get_config().get_peer_id(local_order.initiator))
    {
        LOG_WARN("invalid local order from %d", local_order.initiator);
        return;
    }

    process_local_order(local_order);
    
}

// Themis
void HotStuffBase::process_local_order(const LocalOrder &local_order){
    if(on_receive_local_order(local_order, pmaker->get_parents())==true){
        /* FairPropose() */
        auto fair_proposal = fair_propose();
        auto graph = fair_proposal.first;
        auto e_missing = fair_proposal.second;
        /* FairUpdate() */
        auto e_update = fair_update();
        /* Store proposed commands */
        for(auto g: graph){
            storage->add_to_proposed_cmds_cache(g.first);
        }
        // storage->clear_local_order();
        HOTSTUFF_LOG_DEBUG("[[process_local_order]] [fromR-%d] [thisL-%d] Cleared Local Order", local_order.initiator, get_id());
        /** Create a new proposal block and broadcast to the replicas **/
        pmaker->beat().then([this, graph=std::move(graph), e_missing=std::move(e_missing), e_up=std::move(e_update)](ReplicaID proposer) {
            if (proposer == get_id())
                on_propose(graph, e_missing, e_up, pmaker->get_parents());
        });
    }
}



bool HotStuffBase::conn_handler(const salticidae::ConnPool::conn_t &conn, bool connected) {
    if (connected)
    {
        if (!pn.enable_tls) return true;
        auto cert = conn->get_peer_cert();
        //SALTICIDAE_LOG_INFO("%s", salticidae::get_hash(cert->get_der()).to_hex().c_str());
        return valid_tls_certs.count(salticidae::get_hash(cert->get_der()));
    }
    return true;
}

void HotStuffBase::print_stat() const {
    LOG_INFO("===== begin stats =====");
    LOG_INFO("-------- queues -------");
    LOG_INFO("blk_fetch_waiting: %lu", blk_fetch_waiting.size());
    LOG_INFO("blk_delivery_waiting: %lu", blk_delivery_waiting.size());
    LOG_INFO("decision_waiting: %lu", decision_waiting.size());
    LOG_INFO("-------- misc ---------");
    LOG_INFO("fetched: %lu", fetched);
    LOG_INFO("delivered: %lu", delivered);
    LOG_INFO("cmd_cache: %lu", storage->get_cmd_cache_size());
    LOG_INFO("blk_cache: %lu", storage->get_blk_cache_size());
    LOG_INFO("------ misc (10s) -----");
    LOG_INFO("fetched: %lu", part_fetched);
    LOG_INFO("delivered: %lu", part_delivered);
    LOG_INFO("decided: %lu", part_decided);
    LOG_INFO("gened: %lu", part_gened);
    LOG_INFO("avg. parent_size: %.3f",
            part_delivered ? part_parent_size / double(part_delivered) : 0);
    LOG_INFO("delivery time: %.3f avg, %.3f min, %.3f max",
            part_delivered ? part_delivery_time / double(part_delivered) : 0,
            part_delivery_time_min == double_inf ? 0 : part_delivery_time_min,
            part_delivery_time_max);

    part_parent_size = 0;
    part_fetched = 0;
    part_delivered = 0;
    part_decided = 0;
    part_gened = 0;
    part_delivery_time = 0;
    part_delivery_time_min = double_inf;
    part_delivery_time_max = 0;
#ifdef HOTSTUFF_MSG_STAT
    LOG_INFO("--- replica msg. (10s) ---");
    size_t _nsent = 0;
    size_t _nrecv = 0;
    for (const auto &replica: peers)
    {
        auto conn = pn.get_peer_conn(replica);
        if (conn == nullptr) continue;
        size_t ns = conn->get_nsent();
        size_t nr = conn->get_nrecv();
        size_t nsb = conn->get_nsentb();
        size_t nrb = conn->get_nrecvb();
        conn->clear_msgstat();
        LOG_INFO("%s: %u(%u), %u(%u), %u",
            get_hex10(replica).c_str(), ns, nsb, nr, nrb, part_fetched_replica[replica]);
        _nsent += ns;
        _nrecv += nr;
        part_fetched_replica[replica] = 0;
    }
    nsent += _nsent;
    nrecv += _nrecv;
    LOG_INFO("sent: %lu", _nsent);
    LOG_INFO("recv: %lu", _nrecv);
    LOG_INFO("--- replica msg. total ---");
    LOG_INFO("sent: %lu", nsent);
    LOG_INFO("recv: %lu", nrecv);
#endif
    LOG_INFO("====== end stats ======");
}

HotStuffBase::HotStuffBase(uint32_t blk_size,
                    ReplicaID rid,
                    privkey_bt &&priv_key,
                    NetAddr listen_addr,
                    pacemaker_bt pmaker,
                    EventContext ec,
                    size_t nworker,
                    const Net::Config &netconfig):
        HotStuffCore(rid, std::move(priv_key)),
        listen_addr(listen_addr),
        blk_size(blk_size),
        ec(ec),
        tcall(ec),
        vpool(ec, nworker),
        pn(ec, netconfig),
        pmaker(std::move(pmaker)),

        fetched(0), delivered(0),
        nsent(0), nrecv(0),
        part_parent_size(0),
        part_fetched(0),
        part_delivered(0),
        part_decided(0),
        part_gened(0),
        part_delivery_time(0),
        part_delivery_time_min(double_inf),
        part_delivery_time_max(0)
{
    /* register the handlers for msg from replicas */
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::propose_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::vote_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::req_blk_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::resp_blk_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::local_order_handler, this, _1, _2)); // Themis
    pn.reg_conn_handler(salticidae::generic_bind(&HotStuffBase::conn_handler, this, _1, _2));
    pn.reg_error_handler([](const std::exception_ptr _err, bool fatal, int32_t async_id) {
        try {
            std::rethrow_exception(_err);
        } catch (const std::exception &err) {
            HOTSTUFF_LOG_WARN("network async error: %s\n", err.what());
        }
    });
    pn.start();
    pn.listen(listen_addr);
}

void HotStuffBase::do_broadcast_proposal(const Proposal &prop) {
#ifdef HOTSTUFF_ENABLE_LOG_DEBUG
// #ifdef NOTDEFINE
    // for ( auto const &g: prop.blk->get_graph()) {
    //     HOTSTUFF_LOG_DEBUG("[[do_broadcast_proposal]] [R-%d] [L-%d] key = %.10s", get_id(), prop.proposer, get_hex(g.first).c_str());
    //     for (auto const &tx: g.second){
    //         HOTSTUFF_LOG_DEBUG("[[do_broadcast_proposal]] [R-%d] [L-%d] val = %.10s", get_id(), prop.proposer, get_hex(tx).c_str());
    //     }
    // }

    for (const auto &replica: peers){
        HOTSTUFF_LOG_DEBUG("[[do_broadcast_proposal]] [R-%d] [L-%d] replica = %s", get_id(), prop.proposer, get_hex10(replica).c_str());
    }
#endif

    //MsgPropose prop_msg(prop);
    pn.multicast_msg(MsgPropose(prop), peers);
    //for (const auto &replica: peers)
    //    pn.send_msg(prop_msg, replica);
}

void HotStuffBase::do_vote(ReplicaID last_proposer, const Vote &vote) {
    pmaker->beat_resp(last_proposer)
            .then([this, vote](ReplicaID proposer) {
        if (proposer == get_id())
        {
            //throw HotStuffError("unreachable line");
            on_receive_vote(vote);
        }
        else
            pn.send_msg(MsgVote(vote), get_config().get_peer_id(proposer));
    });
}

// Themis
void HotStuffBase::do_send_local_order(ReplicaID proposer, const LocalOrder &local_order) {
    if (proposer == get_id())
    {
        HOTSTUFF_LOG_DEBUG("[[do_send_local_order]] [R-%d] [L-%d] deliver LocalOrder to itself = %s", get_id(), proposer, local_order);
        process_local_order(local_order);
    }
    else{
        HOTSTUFF_LOG_DEBUG("[[do_send_local_order]] [R-%d] [L-%d] Send LocalOrder to Leader = %s", get_id(), proposer, local_order);
        pn.send_msg(MsgLocalOrder(local_order), get_config().get_peer_id(proposer));
    }
}

void HotStuffBase::do_consensus(const block_t &blk) {
    pmaker->on_consensus(blk);
}

void HotStuffBase::do_decide(Finality &&fin) {
    // part_decided++;
    // state_machine_execute(fin);
    // auto it = decision_waiting.find(fin.cmd_hash);
    // if (it != decision_waiting.end())
    // {
    //     it->second(std::move(fin));
    //     decision_waiting.erase(it);
    //     storage->erase_cmd_dependency(fin.cmd_hash);
    // }

    part_decided++;

    std::unordered_map<const salticidae::uint256_t, hotstuff::HotStuffBase::commit_cb_t>::iterator it;
    {
        std::unique_lock<std::mutex> lock(this->mtx);
        state_machine_execute(fin);
        it = decision_waiting.find(fin.cmd_hash);
        if (it == decision_waiting.end()){
            return;
        }
    }

    it->second(std::move(fin));
    
    {
        std::unique_lock<std::mutex> lock(this->mtx);
        decision_waiting.erase(it);
    }
   
    storage->erase_cmd_dependency(fin.cmd_hash);
}

// Themis
void HotStuffBase::reset_reorder_timer() {
    reorder_timer.del();
    // reorder_timer.add(5);
}


HotStuffBase::~HotStuffBase() {}

void HotStuffBase::start(
        std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&replicas,
        double fairness_parameter,      // Themis
        bool ec_loop) {
    for (size_t i = 0; i < replicas.size(); i++)
    {
        auto &addr = std::get<0>(replicas[i]);
        auto cert_hash = std::move(std::get<2>(replicas[i]));
        valid_tls_certs.insert(cert_hash);
        auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
        HotStuffCore::add_replica(i, peer, std::move(std::get<1>(replicas[i])));
        if (addr != listen_addr)
        {
            peers.push_back(peer);
            pn.add_peer(peer);
            pn.set_peer_addr(peer, addr);
            pn.conn_peer(peer);
        }
    }

    /* ((n - 1) + 1 - 1) / 3 */
    // uint32_t nfaulty = peers.size() / 4;
    uint32_t nfaulty = (peers.size() * ((2*fairness_parameter) -1))/4;
    if (nfaulty == 0)
        LOG_WARN("too few replicas in the system to tolerate any failure");
    on_init(nfaulty, fairness_parameter);       // Themis
    pmaker->init(this);
    if (ec_loop)
        ec.dispatch();

    cmd_pending.reg_handler(ec, [this](cmd_queue_t &q) {
        // HOTSTUFF_LOG_DEBUG("[[cmd_pending.reg_handler]] [R-%d] [L-%d] cmd_pending reg_handler Invoked", get_id(), pmaker->get_proposer());

        std::pair<uint256_t, commit_cb_t> e;

        // TODO: Themis : Add pending decisions into this round local order again, 
        // as they were skipped previously by the leader (due to non majority) ???

        while (q.try_dequeue(e))
        {
            ReplicaID proposer = pmaker->get_proposer();

            const auto &cmd_hash = e.first;
            auto it = decision_waiting.find(cmd_hash);
            if (it == decision_waiting.end())
                it = decision_waiting.insert(std::make_pair(cmd_hash, e.second)).first;
            else
                e.second(Finality(id, 0, 0, 0, cmd_hash, uint256_t()));


            // Themis
            local_order_buffer.push(cmd_hash);
            // HOTSTUFF_LOG_DEBUG("[[cmd_pending.reg_handler]] [R-%d] [L-%d] Push commans to local buffer = %.10s", get_id(), proposer, get_hex(cmd_hash).c_str());

            if(local_order_buffer.size() >= blk_size){
                ReplicaID proposer = pmaker->get_proposer();
                std::vector<uint256_t> cmds;
                for (uint32_t i = 0; i < blk_size; i++)
                {
                    cmds.push_back(local_order_buffer.front());
                    local_order_buffer.pop();
                }

                HOTSTUFF_LOG_DEBUG("[[cmd_pending.reg_handler]] [R-%d] [L-%d] Created List of commands and sending to pacemaker", get_id(), proposer);
// #ifdef HOTSTUFF_ENABLE_LOG_DEBUG
#ifdef NOTDEFINE
                for (uint32_t i = 0; i < blk_size; i++){
                    HOTSTUFF_LOG_DEBUG("[[cmd_pending.reg_handler]] [R-%d] [L-%d] Created List of commands and sending to pacemaker (%d) = %.10s", get_id(), proposer, i, get_hex(cmds[i]).c_str());
                }
#endif
                on_local_order(proposer, cmds);

                return true;
            }
            /*
            if (proposer != get_id()) continue;
            cmd_pending_buffer.push(cmd_hash);
            if (cmd_pending_buffer.size() >= blk_size)
            {
                std::vector<uint256_t> cmds;
                for (uint32_t i = 0; i < blk_size; i++)
                {
                    cmds.push_back(cmd_pending_buffer.front());
                    cmd_pending_buffer.pop();
                }
                pmaker->beat().then([this, cmds = std::move(cmds)](ReplicaID proposer) {
                    if (proposer == get_id())
                        on_propose(cmds, pmaker->get_parents());
                });
                return true;
            }
            */
        }

        return false;
    });

    // /** Initialize and start unproposed Timer **/ 
    // reorder_timer = TimerEvent(ec, [this](TimerEvent &) {
    //     reorder(pmaker->get_proposer());
    //     reset_reorder_timer();
    // });
    // reorder_timer.add(5);
}

}
