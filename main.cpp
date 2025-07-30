#include <iostream>

#include "pgconnection.h"

int main(int argc, char* argv[])
{
    std::cout << "Starting Benchpress\n";

    PGconnection pgconnect("host=localhost port=5432 dbname=pgbench user=pguser password=pgpass");

    return 0;
}

