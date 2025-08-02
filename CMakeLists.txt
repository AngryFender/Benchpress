cmake_minimum_required(VERSION 3.27)
project(Benchpress)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD)
MESSAGE(STATUS "Using toolchain file ➜ ${CMAKE_TOOLCHAIN_FILE}")

find_package(PostgreSQL REQUIRED)
find_package(libpqxx REQUIRED)

get_property(imported_targets DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" PROPERTY IMPORTED_TARGETS)
MESSAGE(STATUS "Imported targets ➜ ${imported_targets}")

add_executable(Benchpress main.cpp
        pgconnection.cpp
        pgconnection.h
        utility.h)

target_link_libraries(Benchpress
        PRIVATE
        libpqxx::pqxx
        PostgreSQL::PostgreSQL
)
