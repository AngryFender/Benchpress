#include <iostream>
#include <thread>
#include <vector>

#include <pqxx/pqxx>
#include "utility.h"
#include "pgconnection.h"

int main(int argc, char* argv[])
{
    std::cout << "Starting Benchpress\n";
    const int max_transaction = 1000;
    const int max_thread = 16;

    std::vector<std::thread> thread_store(max_thread);
    thread_store.reserve(max_thread);

    std::vector<PGconnection> connection_store;
    connection_store.reserve(max_thread);
    for (int j = 0; j < max_thread; ++j)
    {
        connection_store.emplace_back("host=localhost port=5432 dbname=pgbench user=pguser password=pgpass");
    }

    std::vector<pqxx::connection> connection_collection;
    connection_collection.reserve(max_thread);
    for (int j = 0; j < max_thread; ++j)
    {
        connection_collection.emplace_back("host=localhost port=5432 dbname=pgbench user=pguser password=pgpass");
    }

    auto start_time = std::chrono::steady_clock::now();
    for (int j = 0; j < max_thread; ++j)
    {
        const int thread_no = j;
        std::cout<<"thread = "<<std::to_string(thread_no)<<"\n";

        thread_store.emplace_back([&connection_store,&connection_collection, thread_no]()
        {
            for(int i = 0; i < max_transaction; ++i)
            {
                connection_store[thread_no].simpleTransaction("SELECT 1;");
                // pqxx::work txn(connection_collection[thread_no]);
                // txn.exec("SELECT 1;");
                // txn.commit();
            }
        });
    }

    for(auto& thread: thread_store)
    {
        if(thread.joinable())
        {
            thread.join();
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto time_diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time-start_time);

    std::cout << "Transaction = " << max_transaction * max_thread << "\n"
          << " Time taken = " << (double)time_diff.count() / (double)1000 <<"\n"
          << " TPS = " << (double) max_transaction * max_thread* 1000 / (double) time_diff.count() << "\n";
    return 0;
}

