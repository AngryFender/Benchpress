#include <iostream>
#include <thread>
#include <vector>

#include <pqxx/pqxx>
#include "utility.h"
#include "pgconnection.h"

int main(int argc, char* argv[])
{
    std::cout << "Starting Benchpress\n";
    const int max_transaction = 1;
    const int max_thread = 1;
    const int pipeline_no = 1;

    std::vector<std::thread> thread_store(max_thread);
    thread_store.reserve(max_thread);

    std::vector<PGconnection> connection_store;
    connection_store.reserve(max_thread);
    for (int j = 0; j < max_thread; ++j)
    {
        connection_store.emplace_back("host=localhost port=5432 dbname=postgres user=postgres password=postgres");
    }

    std::vector<pqxx::connection> connection_collection;
    connection_collection.reserve(max_thread);
    for (int j = 0; j < max_thread; ++j)
    {
        connection_collection.emplace_back("host=localhost port=5432 dbname=postgres user=postgres password=postgres");
    }

    auto start_time = std::chrono::steady_clock::now();
    for (int j = 0; j < max_thread; ++j)
    {
        const int thread_no = j;
        thread_store.emplace_back([&connection_store,&connection_collection, thread_no, max_transaction]()
        {
            for(int i = 0; i < max_transaction; ++i)
            {
                // connection_store[thread_no].simpleWriteTransaction("INSERT INTO public.test values (DEFAULT, 'abc')");
                // connection_store[thread_no].simpleReadTransaction("SELECT COUNT(*) FROM public.test;");
                // connection_store[thread_no].repeatTransaction("SELECT COUNT(*) FROM public.test;", 2);
                // connection_store[thread_no].repeatTransaction("SELECT NOW()", pipeline_no);
                // connection_store[thread_no].singleTransaction();
																connection_store[thread_no].singlePreparedTransaction();
//                pqxx::work txn(connection_collection[thread_no]);
//                 txn.exec("INSERT INTO public.test values (DEFAULT, 'abc')");
//                 txn.exec("SELECT 1;");
//                 txn.commit();
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

    std::cout << "Transaction = " << max_transaction * max_thread * pipeline_no<< "\n"
          << " Time taken = " << (double)time_diff.count() / (double)1000 <<"\n"
          << " TPS = " << (double) max_transaction * max_thread * pipeline_no * 1000 / (double) time_diff.count() << "\n";
    return 0;
}

