//
// Created by meox on 21/08/16.
//

#ifndef TICKETKAFKA_UTIL_H
#define TICKETKAFKA_UTIL_H

#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <zmq.hpp>

namespace kutil
{
    template <typename C, typename T = typename C::value_type>
    std::vector<std::vector<T>> partions(const C& c, uint n);

    std::string to_string(const zmq::message_t& msg);
}

#endif //TICKETKAFKA_UTIL_H
