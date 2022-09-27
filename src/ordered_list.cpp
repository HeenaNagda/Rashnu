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
 * 
 *          Author: Heena Nagda
 *           Project: Themis
 */

#include "hotstuff/ordered_list.h"

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN
#define LOG_PROTO HOTSTUFF_LOG_PROTO

namespace hotstuff {

    OrderedList::Iterator::Iterator(LinkedNode* new_ptr) : node_ptr(new_ptr) {}

    OrderedList::Iterator::Iterator() : node_ptr(nullptr) {}

    bool OrderedList::Iterator::operator!=(const Iterator& it) const{
        return node_ptr!=it.node_ptr;
    }
    uint256_t OrderedList::Iterator::operator*() const{
        return node_ptr->cmd_hash;
    }

    OrderedList::Iterator OrderedList::Iterator::operator++(int) {
        Iterator it = *this;
        node_ptr = node_ptr->next;
        return it;
    }

    OrderedList::Iterator OrderedList::Iterator::operator+(int i) {
        Iterator it = *this;
        while(i>0){
            it.node_ptr = it.node_ptr->next;
            i--;
        }
        return it;
    }

    OrderedList::Iterator OrderedList::Iterator::next() {
        Iterator it = Iterator(this->node_ptr);
        it.node_ptr = it.node_ptr->next;
        return it;
    }

    OrderedList::OrderedList(){
        /* Head and tail nodes of this list are the dummy nodes */
        head = new LinkedNode(salticidae::get_hash(0));
        tail = new LinkedNode(salticidae::get_hash(0));
        head->next = tail;
        tail->prev = head;
    }

    void OrderedList::push_back(uint256_t cmd_hash){
        if(linked_cache.count(cmd_hash)>0){
            return;
        }
        LinkedNode* node_to_add = new LinkedNode(cmd_hash);
        LinkedNode* last_node = tail->prev;
        last_node->next = node_to_add;
        node_to_add->prev = last_node;
        node_to_add->next = tail;
        tail->prev = node_to_add;
        linked_cache[cmd_hash] = node_to_add;
    }

    void OrderedList::remove(uint256_t cmd_hash){
        if(linked_cache.count(cmd_hash)==0){
            return;
        }
        LinkedNode* node_to_remove = linked_cache[cmd_hash];
        LinkedNode* prev_node = node_to_remove->prev;
        LinkedNode* next_node = node_to_remove->next;
        prev_node->next = next_node;
        next_node->prev = prev_node;
        node_to_remove->prev = nullptr;
        node_to_remove->next = nullptr;
        linked_cache.erase(cmd_hash);
    }

    OrderedList::Iterator OrderedList::begin() const {
        Iterator it = Iterator(head->next);
        return it;
    }

    OrderedList::Iterator OrderedList::end() const {
        Iterator it = Iterator(tail);
        return it;
    }

    size_t OrderedList::get_size(){
        return linked_cache.size();
    }

    std::vector<uint256_t> OrderedList::get_cmds(){
        std::vector<uint256_t> cmds;
        LinkedNode* curr = head->next;
        while(curr!=tail){
            cmds.push_back(curr->cmd_hash);
        }
        return cmds;
    }

    std::vector<std::pair<uint256_t,uint256_t>> OrderedList::get_curr_missing_edges(std::unordered_map<uint256_t, std::unordered_set<uint256_t>>& missing){
        std::vector<std::pair<uint256_t,uint256_t>> curr_missing;
        for(LinkedNode* ptr1 = head->next; ptr1!=tail; ptr1 = ptr1->next){
            auto from_v = ptr1->cmd_hash;
            // HOTSTUFF_LOG_DEBUG("[[get_curr_missing_edges]] from_v = %.10s", from_v.to_hex());
            for(LinkedNode* ptr2 = ptr1->next; ptr2!=tail; ptr2 = ptr2->next){
                auto to_v = ptr2->cmd_hash;
                // HOTSTUFF_LOG_DEBUG("[[get_curr_missing_edges]] to_v = %.10s", from_v.to_hex());
                if(missing[from_v].count(to_v)>0 || missing[to_v].count(from_v)>0) {
                    curr_missing.push_back(std::make_pair(from_v, to_v));
                    // HOTSTUFF_LOG_DEBUG("[[get_curr_missing_edges]] current missing = (%.10s, %.10s)", from_v, to_v.to_hex());
                }
            }
        }
        return curr_missing;
    }
}