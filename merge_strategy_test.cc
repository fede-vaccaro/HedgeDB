#include <cstdint>
#include <vector>
#include <string>
#include <iostream>
#include <set>

struct partition
{
    size_t size{}; // in million elements
};

int main(int argc, char *argv[])
{

    const auto N_INSERTS = std::stoul(argv[1]);
    const auto N_PER_INSERT = std::stoul(argv[2]);

    std::cout << "Number of inserts: " << N_INSERTS << std::endl;

    std::vector<partition> tables;

    auto print_tables = [&tables]()
    {
        for(auto it = tables.begin(); it != tables.end(); ++it)
        {
            auto table = *it;
            
            std::cout << table.size;
            
            if(it != tables.end())
                std::cout << ", ";
        }
        std::cout << std::endl;
    };

    double merge_thresh = 0.5;

    auto find_merges = [merge_thresh](auto tables) mutable -> std::set<size_t> // indices to merge
    {   
        if(tables.size() < 2)
            return {};

        std::set<size_t> indices_to_merge;

        for(int i = tables.size() - 2; i >= 0; --i)
        {
            auto j = i + 1;

            auto& a = tables[i];
            auto& b = tables[j];

            double ratio = static_cast<double>(b.size) / a.size;
            
            if(ratio > merge_thresh)
            {
                a.size += b.size;
                
                tables.erase(tables.begin() + j);

                indices_to_merge.insert(i);
                indices_to_merge.insert(j);
            }
        }

        return indices_to_merge;
    };

    auto apply_merge = [&tables](const auto& indices_to_merge)
    {
        size_t cumulative_size = 0;

        for(auto index : indices_to_merge)
            cumulative_size += tables[index].size;

        for (auto index : indices_to_merge)
            tables.erase(tables.begin() + index);

        tables.push_back({cumulative_size});
    };

    auto print_merge_list = [](const auto& indices_to_merge)
    {
        std::cout << "Merging indices: ";
        for(auto index : indices_to_merge)
            std::cout << index << " ";
        std::cout << std::endl;
    };

    for(int i = 0; i < N_INSERTS; ++i)
    {
        tables.push_back({N_PER_INSERT});
        std::cout << "Begin flush" << std::endl;
        print_tables();

        auto indices_to_merge = find_merges(tables);

        if(!indices_to_merge.empty())
        {    
            print_merge_list(indices_to_merge);
            apply_merge(indices_to_merge);
        }
        
        print_tables();

        std::cout << "End flush" << std::endl;
    }

}