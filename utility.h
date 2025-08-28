#ifndef UTILITY_H
#define UTILITY_H
#include <atomic>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>

std::string currentTime()
{
    auto ct_raw = std::chrono::system_clock::now();
    auto ct_time_t = std::chrono::system_clock::to_time_t(ct_raw);
    auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(ct_raw.time_since_epoch()) %
        std::chrono::seconds(1);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&ct_time_t), "%Y-%m-%d %H:%M:%S") << "." << std::setw(6) << std::setfill('0') <<
        now_ms.count();
    return ss.str();
}

void userInstrumentRequest(std::atomic_int& instrument_id,
                           const int target_instrument_id,
                           std::vector<pqxx::connection>& connection_collection,
                           const int thread_no,
                           const int user_id){

    while(instrument_id.load() < target_instrument_id)
    {
        const int instrument_value = ++instrument_id;
        while(1){
            try {
                pqxx::work txn(connection_collection[thread_no]);
                txn.exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                pqxx::result result = txn.exec_prepared("try_insert", instrument_value, 1, 1, 1, user_id);
                if(result.empty() || result[0][0].as<int>() == 0) {
                    throw pqxx::sql_error();
                }
                txn.commit();
                break;
            } catch (pqxx::sql_error &e) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }


}

#endif //UTILITY_H
