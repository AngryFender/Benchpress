#include "pgconnection.h"

#include <cstring>

#include "libpq-fe.h"
#include <arpa/inet.h>
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
    if (PGRES_COMMAND_OK != PQresultStatus(res2.get()))
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
    if (PGRES_COMMAND_OK != PQresultStatus(res2.get()))
    {
        throw std::runtime_error("Statement failed: " + std::string(PQerrorMessage(_pgconn.get())));
    }
}

void PGconnection::repeatTransaction(const std::string& statement, const int repeat)
{
    try
    {
        // enter pipeline mode
        if (!PQenterPipelineMode(_pgconn.get()))
        {
            throw std::runtime_error("Pipeline enter mode failed: " + std::string(PQerrorMessage(_pgconn.get())));
        }

        // start sending multiple queries
        for (int i = 0; i < repeat; ++i)
        {
            if (!PQsendQueryParams(_pgconn.get(), statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0))
            {
                throw std::runtime_error("Pipeline query failed:" + std::string(PQerrorMessage(_pgconn.get())));
            }
        }

        // flush queries to server
        if (!PQpipelineSync(_pgconn.get()))
        {
            throw std::runtime_error("Pipeline sync failed: " + std::string(PQerrorMessage(_pgconn.get())));
        }

        Result result;
        ExecStatusType status;
        while (PGRES_PIPELINE_SYNC != getStatus(result, _pgconn.get(), status))
        {
            if (result.get() == nullptr)
            {
                continue;
            }
            switch (status)
            {
            case PGRES_TUPLES_OK:
                {
                    /*int nrows = PQntuples(result.get());
                    int nfields = PQnfields(result.get());

                    for (int i = 0; i < nrows; ++i)
                    {
                        for (int j = 0; j < nfields; ++j)
                        {
                            std::cout << PQgetvalue(result.get(), i, j) << ",";
                        }
                        std::cout << "\n";
                    }*/
                    break;
                }

            case PGRES_COMMAND_OK:
                // std::cout << "Command successfully executed\n";
                // break;

            default:
                throw std::runtime_error(
                    "Error reading back result: " + std::string(PQerrorMessage(_pgconn.get()))
                );
            }
        }
    }
    catch (std::exception& e)
    {
        std::cout << e.what() << "\n";
    }
    PQexitPipelineMode(_pgconn.get());
}

void PGconnection::singleTransaction()
{
    int32_t one = htonl(1);
    int32_t two = htonl(2);
    int32_t three = htonl(3);
    int32_t four = htonl(4);

    const char* param_values[4];
    int param_lengths[4];
    int param_formats[4];

    param_values[0] = reinterpret_cast<char*>(&one);
    param_values[1] = reinterpret_cast<char*>(&two);
    param_values[2] = reinterpret_cast<char*>(&three);
    param_values[3] = reinterpret_cast<char*>(&four);

    for (int i = 0; i < 4; ++i)
    {
        param_lengths[i] = sizeof(int32_t);
        param_formats[i] = 1;
    }

    auto result = Result(PQexecParams(_pgconn.get(),
                                      "SELECT public.my_function($1,$2,$3,$4);",
                                      4,
                                      nullptr,
                                      param_values,
                                      param_lengths,
                                      param_formats,
                                      1));

    auto status = PQresultStatus(result.get());
    if (PGRES_TUPLES_OK == status)
    {
        //get result
        if (PQntuples(result.get()) > 0 && PQnfields(result.get()) > 0)
        {
            // Get pointer to binary data
            const char* value = PQgetvalue(result.get(), 0, 0);
            int len = PQgetlength(result.get(), 0, 0);

            if (len == sizeof(int32_t))
            {
                int32_t network_value;
                memcpy(&network_value, value, sizeof(int32_t));

                const int32_t host_value = ntohl(network_value);
                std::cout << "Result: " << host_value << "\n";
            }
        }
    }
    else
    {
        std::cerr << "single transaction failed:\n" << PQerrorMessage(_pgconn.get()) << "\n";
    }
}

ExecStatusType PGconnection::getStatus(Result& result, PGconn* conn, ExecStatusType& status)
{
    result = Result(PQgetResult(conn));
    status = PQresultStatus(result.get());
    return status;
}
