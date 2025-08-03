#include "pgconnection.h"
#include "libpq-fe.h"

#include <iostream>
#include <vector>

void PGconnection::simpleWriteTransaction(const std::string& statement)
{
    Result res = Result(PQexec(_pgconn.get(), "BEGIN;"));
    if (PGRES_COMMAND_OK != PQresultStatus(res.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }

    Result res1 = Result(PQexec(_pgconn.get(), statement.c_str()));
    if (PGRES_COMMAND_OK != PQresultStatus(res1.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }

    Result res2 = Result(PQexec(_pgconn.get(), "COMMIT;"));
    if (PGRES_COMMAND_OK!= PQresultStatus(res2.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }
}
void PGconnection::simpleReadTransaction(const std::string& statement)
{
    Result res = Result(PQexec(_pgconn.get(), "BEGIN;"));
    if (PGRES_COMMAND_OK != PQresultStatus(res.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }

    Result res1 = Result(PQexec(_pgconn.get(), statement.c_str()));
    if (PGRES_TUPLES_OK != PQresultStatus(res1.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }

    Result res2 = Result(PQexec(_pgconn.get(), "COMMIT;"));
    if (PGRES_COMMAND_OK!= PQresultStatus(res2.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }
}

void PGconnection::repeatTransaction(const std::string& statement, const int repeat)
{
//    PQenterPipelineMode(_pgconn.get());
//
//    for(int i = 0; i<repeat ; ++i)
//    {
//
//        if (!PQsendQueryParams(_pgconn.get(),
//                               statement.c_str(), // query string
//                               0,                 // number of parameters
//                               nullptr,           // parameter types
//                               nullptr,           // parameter values
//                               nullptr,           // parameter lengths
//                               nullptr,           // parameter formats
//                               1))                // ask for binary results if desired
//        {
//            throw std::runtime_error("Async statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
//        }
//    }
//
//    if (!PQpipelineSync(_pgconn.get())) {
//        throw std::runtime_error("Pipeline sync failed: " + std::string(PQerrorMessage(_pgconn.get())));
//    }
//
//    // Make sure queries are sent
//    while (PQflush(_pgconn.get())) {
//        fd_set writefds;
//        FD_ZERO(&writefds);
//        int sock = PQsocket(_pgconn.get());
//        FD_SET(sock, &writefds);
//        if(select(sock + 1, NULL, &writefds, NULL, NULL)<0)
//        {
//            throw std::runtime_error("select() failed during flushe");
//        }
//    }
//    bool done = false;
//    int result_count = 0;
//    while (!done)
//    {
//        if(0 == PQconsumeInput(_pgconn.get()))
//        {
//            //consume failed;
//            break;
//        }
//
//        while (!PQisBusy(_pgconn.get())) {
//            PGresult *res = PQgetResult(_pgconn.get());
//            if (!res)
//            {
//                done = true;
//                break;
//            }
//
//            PQclear(res);
//            ++result_count;
//            std::cout << "result count = " << result_count << "\n";
//        }
//        if (done)
//        {
//            fd_set readfds;
//            FD_ZERO(&readfds);
//            int sock = PQsocket(_pgconn.get());
//            FD_SET(sock, &readfds);
//            if (select(sock + 1, &readfds, nullptr, nullptr, nullptr) < 0) {
//                throw std::runtime_error("select() failed during read");
//            }
//            PQexitPipelineMode(_pgconn.get());
//        }
//    }
//    if (result_count != repeat) {
//        throw std::runtime_error("Expected " + std::to_string(repeat) +
//                                 " results, but got " + std::to_string(result_count));
//    }
//    PQexitPipelineMode(_pgconn.get());
}
