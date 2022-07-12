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

#ifndef _THEMIS_ORDERED_LIST_H
#define _THEMIS_ORDERED_LIST_H

#include <unordered_map>
#include "salticidae/stream.h"

namespace hotstuff {

struct LinkedNode
{
    uint256_t cmd_hash;
    LinkedNode* next;
    LinkedNode* prev;

    LinkedNode(const uint256_t cmd_hash);
};

class OrderedList{
    public:
    class Iterator{
        
        private:
        LinkedNode* node_ptr;         

        public:
        Iterator();
        Iterator(LinkedNode* new_ptr);
        bool operator!=(const Iterator& it) const;
        uint256_t operator*() const;
        Iterator operator++(int);
        Iterator operator+(int i);
        Iterator next();
    };

    private:
    std::unordered_map<uint256_t, LinkedNode*> linked_cache;
    LinkedNode* head;
    LinkedNode* tail;

    public:
    OrderedList();
    void push_back(uint256_t cmd_hash);
    LinkedNode* get_head();
    void remove(uint256_t cmd_hash);
    Iterator begin() const;
    Iterator end() const;
};

}
#endif