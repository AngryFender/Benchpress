#include "pgconnection.h"
#include "libpq-fe.h"
#include <iostream>
#include <vector>
#include "convert.h"

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

void PGconnection::test(std::vector<std::atomic_int> &instrument_counter, std::vector<int> &instrument_limiter)
{
    int32_t user_id         = host_to_network(1);
    int32_t instrument_id   = host_to_network(1);
    int32_t vendor_id       = host_to_network(1);
    int32_t ccd_id          = host_to_network(1);
    int32_t app_id          = host_to_network(1);

    const char* param_values[5];
    int param_lengths[5];
    int param_formats[5];

    param_values[1] = reinterpret_cast<char*>(&vendor_id);
    param_values[2] = reinterpret_cast<char*>(&ccd_id);
    param_values[4] = reinterpret_cast<char*>(&app_id);

    for (int i = 0; i < 5; ++i)
    {
        param_lengths[i] = sizeof(int32_t);
        param_formats[i] = 1;
    }

    // iterate users
    for(int u = 1; u <= 200; ++u)
    {
        int i = 0;
        // iterate instruments
        while(atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]){

            user_id         = host_to_network(u);
            instrument_id   = host_to_network(i);

            param_values[0] = reinterpret_cast<char*>(&user_id);
            param_values[3] = reinterpret_cast<char*>(&instrument_id);

            auto result = Result(PQexecPrepared(_pgconn.get(),
                                                "insert_i",
                                                5,
                                                param_values,
                                                param_lengths,
                                                param_formats,
                                                1));

            auto status = PQresultStatus(result.get());
            if (PGRES_TUPLES_OK == status)
            {
                //get result
                /*if (PQntuples(result.get()) > 0 && PQnfields(result.get()) > 0)
                {
                    // Get pointer to binary data
                    const char* value = PQgetvalue(result.get(), 0, 0);
                    int len = PQgetlength(result.get(), 0, 0);

                    if (len == sizeof(int32_t))
                    {
                        int32_t network_value;
                        memcpy(&network_value, value, sizeof(int32_t));

                        const int32_t host_value = network_to_host(network_value);
                        std::cout << "Result: " << host_value << "\n";
                    }
                }*/
            }
            else
            {
                std::cerr << "single transaction failed:\n" << PQerrorMessage(_pgconn.get()) << "\n";
            }
        }
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

void PGconnection::prepareStatement(const std::string &name, const std::string &statement) {
    Result result(PQprepare(_pgconn.get(), name.c_str(), statement.c_str(), 5, nullptr));
    if(PGRES_COMMAND_OK != PQresultStatus(result.get() )){
        throw std::logic_error("prepare failed");
    }
}

ExecStatusType PGconnection::getStatus(Result& result, PGconn* conn, ExecStatusType& status)
{
    result = Result(PQgetResult(conn));
    status = PQresultStatus(result.get());
    return status;
}

void
PGconnection::testPipeline(std::vector<std::atomic_int> &instrument_counter, std::vector<int> &instrument_limiter,const int pipeline_pack) {

    int32_t user_id         = host_to_network(1);
    int32_t instrument_id   = host_to_network(1);
    int32_t vendor_id       = host_to_network(1);
    int32_t ccd_id          = host_to_network(1);
    int32_t app_id          = host_to_network(1);

    const char* param_values[5];
    int param_lengths[5];
    int param_formats[5];

    param_values[1] = reinterpret_cast<char*>(&vendor_id);
    param_values[2] = reinterpret_cast<char*>(&ccd_id);
    param_values[4] = reinterpret_cast<char*>(&app_id);

    for (int i = 0; i < 5; ++i)
    {
        param_lengths[i] = sizeof(int32_t);
        param_formats[i] = 1;
    }

    // iterate users
    for(int u = 1; u <= 200; ++u)
    {
        int i = 0;
        int query_count = 0;

        while(instrument_counter[u].load() <= instrument_limiter[u]) {
            query_count = 0;

            // enter pipeline mode
            if (!PQenterPipelineMode(_pgconn.get())) {
                throw std::runtime_error("Pipeline enter mode failed: " + std::string(PQerrorMessage(_pgconn.get())));
            }

            // query instruments

            //for(int i = 0; i < pipeline_pack; ++i){

            //1
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }


            //10
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }

            //20
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }

            //30
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }

            //40
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if (atomic_increased_value(instrument_counter[u], i) <= instrument_limiter[u]) {
                ++query_count;
                user_id = host_to_network(u); instrument_id = host_to_network(i);
                param_values[0] = reinterpret_cast<char *>(&user_id); param_values[3] = reinterpret_cast<char *>(&instrument_id);
                PQsendQueryPrepared(_pgconn.get(), "insert_i", 5, param_values, param_lengths, param_formats, 1);
            }
            if(0 == query_count){ break; }

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
                    case PGRES_TUPLES_OK: break;
                    case PGRES_COMMAND_OK: break;
                    default:
                        throw std::runtime_error(
                                "Error reading back result: " + std::string(PQerrorMessage(_pgconn.get()))
                        );
                }
            }
        }
    }
}

