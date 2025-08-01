#ifndef PGCONNECTION_H
#define PGCONNECTION_H
#include <memory>
#include <stdexcept>
#include <string>
#include "libpq-fe.h"

template <typename T, void (*func)(T*)>
struct PGdeleter
{
    void operator()(T* conn)
    {
        if (conn)
        {
            func(conn);
        }
    }
};

using Connection = std::unique_ptr<PGconn, PGdeleter<PGconn, PQfinish>>;
using Result = std::unique_ptr<PGresult, PGdeleter<PGresult, PQclear>>;

class PGconnection
{
public:
    PGconnection(const std::string& dbname): _pgconn(Connection(PQconnectdb(dbname.c_str())))
    {
        if (CONNECTION_OK != PQstatus(_pgconn.get()))
        {
            throw std::runtime_error("Failed to Connection to database,"+ std::string(PQerrorMessage(_pgconn.get())));
        }

        // Set always-secure search path, so malicious users can't take control
        Result res = Result(PQexec(_pgconn.get(),"SELECT pg_catalog.set_config('search_path', '', false)"));
        if (PGRES_TUPLES_OK != PQresultStatus(res.get()))
        {
            throw std::runtime_error("Failed to secure search path ,"+ std::string(PQerrorMessage(_pgconn.get())));
        }

        // Set non-blocking mode
        if (0 != PQsetnonblocking(_pgconn.get(), 1)) {
            throw std::runtime_error("Failed to set non-blocking ,"+ std::string(PQerrorMessage(_pgconn.get())));
        }
    }

    PGconnection(const PGconnection&& connection) noexcept
    {
        if(this != &connection)
        {
            this->_pgconn = std::move(_pgconn);
        }
    }
    PGconnection(const PGconnection& connection) = delete;
    PGconnection& operator=(const PGconnection&) = delete;
    PGconnection& operator=(const PGconnection&& connection)
    {
        if(this != &connection)
        {
            this->_pgconn = std::move(_pgconn);
        }
        return *this;
    };

    void simpleTransaction(const std::string& statement);
    void repeatTransaction(const std::string& statement, const int repeat);

private:
    Connection _pgconn;
};


#endif //PGCONNECTION_H
