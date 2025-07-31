#ifndef UTILITY_H
#define UTILITY_H
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


#endif //UTILITY_H
