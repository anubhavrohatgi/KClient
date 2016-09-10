//
// Created by meox on 20/08/16.
//

#include <iostream>
#include <memory>
#include <vector>
#include <set>
#include <map>
#include <algorithm>

#include "util.h"
#include <gtest/gtest.h>


TEST(partions, check_size)
{
    std::set<int> s{1, 2, 3, 4, 5, 6, 7, 8, 9, 10 , 11};

    for (uint32_t i = 1; i < s.size(); i++)
    {
        auto r = kutil::partions(s, i);
        ASSERT_EQ(r.size(), i);
    }
}

TEST(partions, check_tot_size)
{
    std::set<int> s{1, 2, 3, 4, 5, 6, 7, 8, 9, 10 , 11};

    auto r = kutil::partions(s, 5);
    uint tot{};
    for (auto& x : r)
        tot += x.size();

    ASSERT_EQ(tot, s.size());
}

int main(int argc, char* argv[])
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}