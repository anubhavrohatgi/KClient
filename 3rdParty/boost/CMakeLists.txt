ADD_LIBRARY (IMPORTED_BOOST SHARED IMPORTED GLOBAL)

INCLUDE(ExternalProject)

set( Boost_Bootstrap_Command )
if( UNIX )
	set( Boost_Bootstrap_Command ./bootstrap.sh )
	set( Boost_b2_Command ./b2 )
else()
	if( WIN32 )
		set( Boost_Bootstrap_Command bootstrap.bat )
		set( Boost_b2_Command b2.exe )
	endif()
endif()


ExternalProject_Add(boost
	URL "http://downloads.sourceforge.net/project/boost/boost/1.62.0/boost_1_62_0.tar.bz2?r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Fboost%2Ffiles%2Fboost%2F1.62.0%2F&ts=1475262433&use_mirror=vorboss"
	DOWNLOAD_NAME "boost_1_62_0.tar.bz2"
	URL_MD5 5fb94629535c19e48703bdb2b2e9490f
	BUILD_IN_SOURCE 1
	UPDATE_COMMAND ""
	PATCH_COMMAND ""
	CONFIGURE_COMMAND ${Boost_Bootstrap_Command}
	BUILD_COMMAND  ${Boost_b2_Command} install
	--without-python
	--without-mpi
	--disable-icu
	--prefix=${CMAKE_BINARY_DIR}/INSTALL
	--threading=single,multi
	--link=shared
	--variant=release
	-j8
	INSTALL_COMMAND ""
	INSTALL_COMMAND ${Boost_b2_Command} install 
	    --without-python
	    --without-mpi
	    --disable-icu
	    --prefix=${CMAKE_BINARY_DIR}/INSTALL
	    --threading=single,multi
	    --link=shared
	    --variant=release
	    -j8
	INSTALL_DIR ${CMAKE_BINARY_DIR}/INSTALL/lib
)

ExternalProject_Get_Property(boost INSTALL_DIR)

if( NOT WIN32 )
  set(Boost_LIBRARY_DIR ${CMAKE_BINARY_DIR}/INSTALL/lib/ )
  set(Boost_INCLUDE_DIR ${CMAKE_BINARY_DIR}/INSTALL/include/ )
else()
  set(Boost_LIBRARY_DIR ${CMAKE_BINARY_DIR}/INSTALL/lib/ )
  set(Boost_INCLUDE_DIR ${CMAKE_BINARY_DIR}/INSTALL/include/boost-1_62/ )
endif()


ADD_DEPENDENCIES (IMPORTED_BOOST boost)

MESSAGE (STATUS "Boost_LIBRARY_DIR IS ${Boost_LIBRARY_DIR}")
MESSAGE (STATUS "Boost_INCLUDE_DIR IS ${Boost_INCLUDE_DIR}")

SET_TARGET_PROPERTIES (IMPORTED_BOOST PROPERTIES IMPORTED_LOCATION ${Boost_LIBRARY_DIR} INTERFACE_INCLUDE_DIRECTORIES ${Boost_INCLUDE_DIR})
