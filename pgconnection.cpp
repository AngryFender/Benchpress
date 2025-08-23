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
    try
    {
        // enter pipeline mode
        if(!PQenterPipelineMode(_pgconn.get()))
        {
            throw std::runtime_error("Pipeline enter mode failed: " + std::string(PQerrorMessage(_pgconn.get())));
        }

        // start sending multiple queries
        for(int i = 0; i < repeat; ++i)
        {
            if (!PQsendQueryParams(_pgconn.get(), statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0))
            {
                throw std::runtime_error("Pipeline query failed:" + std::string(PQerrorMessage(_pgconn.get())));
            }
        }

        // flush queries to server
        if (!PQpipelineSync(_pgconn.get())) {
            throw std::runtime_error("Pipeline sync failed: " + std::string(PQerrorMessage(_pgconn.get())));
        }

        std::cout << "status of the pipeline " << PQpipelineStatus(_pgconn.get()) << "\n";

        static int id = 1;
        Result result;
        while((result = std::move(Result(PQgetResult(_pgconn.get())))))
        {
            std::cout<<"id: "<<id++<<"\n";

            ExecStatusType status = PQresultStatus(result.get());
            if(PGRES_TUPLES_OK == status)
            {
                int nrows = PQntuples(result.get());
                int nfields = PQnfields(result.get());

                for(int i = 0; i < nrows; ++i)
                {
                    for(int j = 0; j < nfields; ++j)
                    {
                        std::cout<<PQgetvalue(result.get(),i, j)<<",";
                    }
                    std::cout<<"\n";
                }
            }
            else if(PGRES_COMMAND_OK == status)
            {
                std::cout<<"Command succesfully excuted\n";
            }else
            {
                throw std::runtime_error("Error reading back result: " + std::string(PQerrorMessage(_pgconn.get())));
            }
            Result result1 = std::move(Result(PQgetResult(_pgconn.get())));
        }
    }
    catch (std::exception& e)
    {
        std::cout<<e.what()<<"\n";
    }
    PQexitPipelineMode(_pgconn.get());
}
