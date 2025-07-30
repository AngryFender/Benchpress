#ifndef PGCONNECTION_H
#define PGCONNECTION_H
#include <memory>
#include <stdexcept>
#include <string>
#include "libpq-fe.h"

template <typename T, void (*func)(T*)>
struct PGdelelter
{
    void operator()(T* conn)
    {
        if (conn)
        {
            func(conn);
        }
    }
};

using Connection = std::unique_ptr<PGconn, PGdelelter<PGconn, PQfinish>>;
using Result = std::unique_ptr<PGresult, PGdelelter<PGresult, PQclear>>;

class PGconnection
{
public:
    PGconnection(const std::string& dbname): _pgconn(Connection(PQconnectdb(dbname.c_str())))
    {
        if (CONNECTION_OK != PQstatus(_pgconn.get()))
        {
            throw std::runtime_error("Failed to Connection to database,"+ std::string(PQerrorMessage(_pgconn.get())));
        }

        /* Set always-secure search path, so malicious users can't take control. */
        // Result res = PQexec(&_pgconn.get(),
        //              "SELECT pg_catalog.set_config('search_path', '', false)");
        // if (PQresultStatus(res) != PGRES_TUPLES_OK)
        // {
        //     fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
        //     PQclear(res);
        //     exit_nicely(conn);
        // }

        /*
         * Should PQclear PGresult whenever it is no longer needed to avoid memory
         * leaks
         */
        // PQclear(res);

    }

private:
    Connection _pgconn;
};


#endif //PGCONNECTION_H
