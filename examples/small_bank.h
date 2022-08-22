#ifndef __SMALL_BANK_H__
#define __SMALL_BANK_H__

#include <vector>
#include <stdlib.h>
#include <time.h>
#include <limits>
#include <iostream>
#include <random>

#include "salticidae/util.h"
#include "cpp_random_distributions/zipfian_int_distribution.h"


#define TX_TYPES 7
#define MIN_SPLIT_TX_PARTY_SIZE 3
#define MAX_SPLIT_TX_PARTY_SIZE 10


class SmallBank{
private:
    uint64_t n_users;
    std::vector<uint64_t> checking_accounts;
    std::vector<uint64_t> saving_accounts;
public:
    SmallBank(uint64_t n_users);
    /* tx_type = 0 */
    void transaction_savings(uint64_t user_id, uint64_t amount);
    /* tx_type = 1 */    
    void deposit_checking(uint64_t user_id, uint64_t amount);
    /* tx_type = 2 */
    void write_check(uint64_t user_id, uint64_t amount);
    /* tx_type = 3 */
    void send_payment(uint64_t from_user_id, uint64_t to_user_id, uint64_t amount);
    /* tx_type = 4 */
    void transaction_split(std::vector<std::pair<uint64_t, uint64_t>> payors, std::vector<uint64_t> party);
    /* tx_type = 5 */
    void amalgamate(uint64_t user_id);
    /* tx_type = 6 */
    std::pair<uint64_t,uint64_t> query(uint64_t user_id);
};

class SmallBankManager{
private:
    SmallBank *bank;

    uint64_t n_users;
    double prob_choose_mtx;                 /* Prob of choosing modifying transaction */
    double skew_factor;                     /* Skewness for zipfian distribution */

    std::default_random_engine tx_generator;
    std::bernoulli_distribution tx_distribution;

    std::default_random_engine mtx_generator;
    std::pair<uint64_t, uint64_t> random_mtx;
    std::uniform_int_distribution<uint64_t> mtx_distribution;

    std::default_random_engine user_generator;
    std::pair<uint64_t, uint64_t> random_users;
    zipfian_int_distribution<uint64_t> user_distribution;

    std::vector<uint64_t> get_next_transaction_by_type(uint64_t tx_type);
    uint64_t random_number_generator(uint64_t min, uint64_t max);
    std::pair<uint64_t,uint64_t> show_account_info(uint64_t user_id);

public:
    SmallBankManager(uint64_t n_users, double prob_choose_mtx, double skew_factor);
    std::vector<uint64_t> get_next_transaction_serialized();
    std::pair<uint64_t, uint64_t> execute_transaction(const uint64_t* tx_payload);

    // /* Just for testing they are public */
    // std::vector<uint64_t> get_next_transaction_by_type(uint64_t tx_type);
    // uint64_t random_number_generator(uint64_t min, uint64_t max);
    // std::pair<uint64_t,uint64_t> show_account_info(uint64_t user_id);
    
};

#endif