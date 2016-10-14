//
// Created by meox on 25/09/16.
//

#include <sys/time.h>
#include "util.h"


namespace kutil
{
	std::string to_string(const zmq::message_t &msg)
	{
		return std::string(reinterpret_cast<const char*>(msg.data()), msg.size());
	}

	void print_time()
	{
		timeval tv;
		char buf[64];
		gettimeofday(&tv, NULL);
		strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
		fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
	}
}