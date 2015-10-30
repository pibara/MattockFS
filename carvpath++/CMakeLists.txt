cmake_minimum_required(VERSION 2.6)
enable_testing()
project(carvpath++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
include_directories(".")
set(prefix      ${CMAKE_INSTALL_PREFIX})
set(exec_prefix ${CMAKE_INSTALL_PREFIX})
set(libdir      ${CMAKE_INSTALL_PREFIX}/lib)
set(includedir  ${CMAKE_INSTALL_PREFIX}/include)
ADD_DEFINITIONS( -g -Wall -Werror -Weffc++ -Wctor-dtor-privacy -Wuninitialized -Winit-self -Wmissing-include-dirs -Wfloat-equal -Wshadow -Wpointer-arith )
SET(INC_SEARCHPATH
  /usr/include
  /usr/local/include
)
FIND_PATH(B2_INCLUDE_PATH 
          NAMES blake2.h 
          PATHS ${INC_SEARCHPATH})
SET(LIB_SEARCHPATH
  /usr/lib
  /usr/local/lib
)
FIND_LIBRARY(B2_LIBRARY b2 ${LIB_SEARCHPATH})
IF(B2_LIBRARY)
  IF(B2_INCLUDE_PATH)
    message(STATUS "Found b2 library and header.")
  ELSE()
    message(FATAL_ERROR "Found the b2 library but not the relevant header files.")
  ENDIF()
ELSE()
  message(FATAL_ERROR "No b2 lib found.")
ENDIF()

add_executable(testflatten testflatten.cpp)
target_link_libraries(testflatten b2)
add_test(testflatten testflatten)

add_executable(testrange testrange.cpp)
target_link_libraries(testrange b2)
add_test(testrange testrange)

install(FILES carvpath.hpp DESTINATION include)
