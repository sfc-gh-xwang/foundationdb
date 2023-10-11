/*
 * networktest.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cmath>

#include "fmt/format.h"
#include "fdbserver/NetworkTest.h"
#include "flow/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/UnitTest.h"
#include <inttypes.h>

#include "flow/IConnection.h"
#include "flow/actorcompiler.h" // This must be the last #include.

constexpr int WLTOKEN_NETWORKTEST = WLTOKEN_FIRST_AVAILABLE;

struct LatencyStats {
	using sample = double;
	double x = 0;
	double x2 = 0;
	double n = 0;

	sample tick() {
		// now() returns the timestamp when we were scheduled; count
		// all that time against this sample.
		return now();
	}

	void tock(sample tick) {
		// time_monotonic returns the timestamp when it was called;
		// count the time it took us to be dispatched and invoke
		// timer_monotonic
		double delta = timer_monotonic() - tick;
		x += delta;
		x2 += (delta * delta);
		n++;
	}

	void reset() { *this = LatencyStats(); }
	double count() { return n; }
	double mean() { return x / n; }
	double stddev() { return sqrt(x2 / n - (x / n) * (x / n)); }
};

NetworkTestInterface::NetworkTestInterface(NetworkAddress remote)
  : test(Endpoint::wellKnown({ remote }, WLTOKEN_NETWORKTEST)),
    testStream(Endpoint::wellKnown({ remote }, WLTOKEN_NETWORKTEST + 1)),
    start(Endpoint::wellKnown({ remote }, WLTOKEN_NETWORKTEST + 2)) {}

NetworkTestInterface::NetworkTestInterface(INetwork* local) {
	test.makeWellKnownEndpoint(WLTOKEN_NETWORKTEST, TaskPriority::DefaultEndpoint);
	testStream.makeWellKnownEndpoint(WLTOKEN_NETWORKTEST + 1, TaskPriority::DefaultEndpoint);
	start.makeWellKnownEndpoint(WLTOKEN_NETWORKTEST + 2, TaskPriority::DefaultEndpoint);
}

ACTOR Future<Void> networkTestServer() {
	state NetworkTestInterface interf(g_network);
	state Future<Void> logging = delay(1.0);
	state double lastTime = now();
	state int sent = 0;
	state LatencyStats latency;

	loop {
		choose {
			when(NetworkTestRequest req = waitNext(interf.test.getFuture())) {
				LatencyStats::sample sample = latency.tick();
				req.reply.send(NetworkTestReply(Value(std::string(req.replySize, '.'))));
				latency.tock(sample);
				sent++;
			}
			when(wait(logging)) {
				auto spd = sent / (now() - lastTime);
				if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
					fprintf(stderr, "%f\t%.3f\t%.3f\n", spd, latency.mean() * 1e6, latency.stddev() * 1e6);
				} else {
					fprintf(stderr, "responses per second: %f (%f us)\n", spd, latency.mean() * 1e6);
				}
				latency.reset();
				lastTime = now();
				sent = 0;
				logging = delay(1.0);
			}
		}
	}
}

ACTOR Future<Void> networkTestStreamingServer() {
	state NetworkTestInterface interf(g_network);
	state Future<Void> logging = delay(1.0);
	state double lastTime = now();
	state int sent = 0;
	state LatencyStats latency;

	loop {
		try {
			choose {
				when(state NetworkTestStreamingRequest req = waitNext(interf.testStream.getFuture())) {
					state LatencyStats::sample sample = latency.tick();
					state int i = 0;
					for (; i < 100; ++i) {
						wait(req.reply.onReady());
						req.reply.send(NetworkTestStreamingReply{ i });
					}
					req.reply.sendError(end_of_stream());
					latency.tock(sample);
					sent++;
				}
				when(wait(logging)) {
					auto spd = sent / (now() - lastTime);
					if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
						fprintf(stderr, "%f\t%.3f\t%.3f\n", spd, latency.mean() * 1e6, latency.stddev() * 1e6);
					} else {
						fprintf(stderr, "responses per second: %f (%f us)\n", spd, latency.mean() * 1e6);
					}
					latency.reset();
					lastTime = now();
					sent = 0;
					logging = delay(1.0);
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_operation_obsolete) {
				throw e;
			}
		}
	}
}

static bool moreRequestsPending(int count) {
	if (count == -1) {
		return false;
	} else {
		int request_count = FLOW_KNOBS->NETWORK_TEST_REQUEST_COUNT;
		return (!request_count) || count < request_count;
	}
}

static bool moreLoggingNeeded(int count, int iteration) {
	if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
		return iteration <= 2;
	} else {
		return moreRequestsPending(count);
	}
}

ACTOR Future<Void> testClient(std::vector<NetworkTestInterface> interfs,
                              int* sent,
                              int* completed,
                              LatencyStats* latency) {
	state std::string request_payload(FLOW_KNOBS->NETWORK_TEST_REQUEST_SIZE, '.');
	state LatencyStats::sample sample;

	while (moreRequestsPending(*sent)) {
		(*sent)++;
		sample = latency->tick();
		NetworkTestReply rep = wait(
		    retryBrokenPromise(interfs[deterministicRandom()->randomInt(0, interfs.size())].test,
		                       NetworkTestRequest(StringRef(request_payload), FLOW_KNOBS->NETWORK_TEST_REPLY_SIZE)));
		latency->tock(sample);
		(*completed)++;
	}
	return Void();
}

ACTOR Future<Void> testClientStream(std::vector<NetworkTestInterface> interfs,
                                    int* sent,
                                    int* completed,
                                    LatencyStats* latency) {
	state std::string request_payload(FLOW_KNOBS->NETWORK_TEST_REQUEST_SIZE, '.');
	state LatencyStats::sample sample;

	while (moreRequestsPending(*sent)) {
		(*sent)++;
		sample = latency->tick();
		state ReplyPromiseStream<NetworkTestStreamingReply> stream =
		    interfs[deterministicRandom()->randomInt(0, interfs.size())].testStream.getReplyStream(
		        NetworkTestStreamingRequest{});
		state int j = 0;
		try {
			loop {
				NetworkTestStreamingReply rep = waitNext(stream.getFuture());
				ASSERT(rep.index == j++);
			}
		} catch (Error& e) {
			ASSERT(e.code() == error_code_end_of_stream || e.code() == error_code_connection_failed ||
			       e.code() == error_code_request_maybe_delivered);
		}
		latency->tock(sample);
		(*completed)++;
	}
	return Void();
}

ACTOR Future<Void> logger(int* sent, int* completed, LatencyStats* latency) {
	state double lastTime = now();
	state int logged = 0;
	state int iteration = 0;
	while (moreLoggingNeeded(logged, ++iteration)) {
		wait(delay(1.0));
		auto spd = (*completed - logged) / (now() - lastTime);
		if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
			if (iteration == 2) {
				// We don't report the first iteration because of warm-up effects.
				printf("%f\t%.3f\t%.3f\n", spd, latency->mean() * 1e6, latency->stddev() * 1e6);
			}
		} else {
			fprintf(stderr, "messages per second: %f (%6.3f us)\n", spd, latency->mean() * 1e6);
		}
		latency->reset();
		lastTime = now();
		logged = *completed;
	}
	// tell the clients to shut down
	*sent = -1;
	return Void();
}

static void networkTestnanosleep() {
	printf("nanosleep speed test\n");

#ifdef __linux__
	printf("\nnanosleep(10) latencies:");
	for (int i = 0; i < 10; i++) {

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before) * 1e6);
	}

	printf("\nnanosleep(10) latency after 5ms spin:");
	for (int i = 0; i < 10; i++) {
		double a = timer_monotonic() + 5e-3;
		while (timer_monotonic() < a) {
		}

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();
		printf(" %0.3lf", (after - before) * 1e6);
	}

	printf("\nnanosleep(20000) latency:");
	for (int i = 0; i < 10; i++) {
		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before) * 1e6);
	}
	printf("\n");

	printf("nanosleep(20000) loop\n");
	while (true) {
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, nullptr);
	}
#endif

	return;
}

ACTOR Future<Void> networkTestClient(std::string testServers) {
	if (testServers == "nanosleep") {
		networkTestnanosleep();
		// return Void();
	}

	state std::vector<NetworkTestInterface> interfs;
	state std::vector<NetworkAddress> servers = NetworkAddress::parseList(testServers);
	state int sent = 0;
	state int completed = 0;
	state LatencyStats latency;

	interfs.reserve(servers.size());
	for (int i = 0; i < servers.size(); i++) {
		interfs.push_back(NetworkTestInterface(servers[i]));
	}

	state std::vector<Future<Void>> clients;
	clients.reserve(FLOW_KNOBS->NETWORK_TEST_CLIENT_COUNT);
	for (int i = 0; i < FLOW_KNOBS->NETWORK_TEST_CLIENT_COUNT; i++) {
		clients.push_back(testClient(interfs, &sent, &completed, &latency));
	}
	clients.push_back(logger(&sent, &completed, &latency));

	wait(waitForAll(clients));
	return Void();
}

struct RandomIntRange {
	int min;
	int max;

	RandomIntRange(int low = 0, int high = 0) : min(low), max(high) {}

	// Accepts strings of the form "min:max" or "N"
	// where N will be used for both min and max
	RandomIntRange(std::string str) {
		StringRef high = str;
		StringRef low = high.eat(":");
		if (high.size() == 0) {
			high = low;
		}
		min = low.size() == 0 ? 0 : atol(low.toString().c_str());
		max = high.size() == 0 ? 0 : atol(high.toString().c_str());
		if (min > max) {
			std::swap(min, max);
		}
	}

	int get() const { return (max == 0) ? 0 : nondeterministicRandom()->randomInt(min, max + 1); }

	std::string toString() const { return format("%d:%d", min, max); }
};

struct P2PNetworkTest {
	// Addresses to listen on
	std::vector<Reference<IListener>> listeners;
	// Addresses to randomly connect to
	std::vector<NetworkAddress> remotes;
	// Number of outgoing connections to maintain
	int connectionsOut;
	// Message size range to send on outgoing established connections
	RandomIntRange requestBytes;
	// Message size to reply with on incoming established connections
	RandomIntRange replyBytes;
	// Number of requests/replies per session
	RandomIntRange requests;
	// Delay after message send and receive are complete before closing connection
	RandomIntRange idleMilliseconds;
	// Random delay before socket reads
	RandomIntRange waitReadMilliseconds;
	// Random delay before socket writes
	RandomIntRange waitWriteMilliseconds;

	double startTime;
	int64_t bytesSent;
	int64_t bytesReceived;
	int sessionsIn;
	int sessionsOut;
	int connectErrors;
	int acceptErrors;
	int sessionErrors;

	Standalone<StringRef> msgBuffer;

	std::string statsString() {
		double elapsed = now() - startTime;
		std::string s = format(
		    "%.2f MB/s bytes in  %.2f MB/s bytes out  %.2f/s completed sessions in  %.2f/s completed sessions out  ",
		    bytesReceived / elapsed / 1e6,
		    bytesSent / elapsed / 1e6,
		    sessionsIn / elapsed,
		    sessionsOut / elapsed);
		s += format("Total Errors %d  connect=%d  accept=%d  session=%d",
		            connectErrors + acceptErrors + sessionErrors,
		            connectErrors,
		            acceptErrors,
		            sessionErrors);
		bytesSent = 0;
		bytesReceived = 0;
		sessionsIn = 0;
		sessionsOut = 0;
		startTime = now();
		return s;
	}

	P2PNetworkTest() {}

	P2PNetworkTest(std::string listenerAddresses,
	               std::string remoteAddresses,
	               int connectionsOut,
	               RandomIntRange sendMsgBytes,
	               RandomIntRange recvMsgBytes,
	               RandomIntRange requests,
	               RandomIntRange idleMilliseconds,
	               RandomIntRange waitReadMilliseconds,
	               RandomIntRange waitWriteMilliseconds)
	  : connectionsOut(connectionsOut),
	    requestBytes(sendMsgBytes),
	    replyBytes(recvMsgBytes),
	    requests(requests),
	    idleMilliseconds(idleMilliseconds),
	    waitReadMilliseconds(waitReadMilliseconds),
	    waitWriteMilliseconds(waitWriteMilliseconds) {
		bytesSent = 0;
		bytesReceived = 0;
		sessionsIn = 0;
		sessionsOut = 0;
		connectErrors = 0;
		acceptErrors = 0;
		sessionErrors = 0;
		msgBuffer = makeString(std::max(sendMsgBytes.max, recvMsgBytes.max));

		if (!remoteAddresses.empty()) {
			remotes = NetworkAddress::parseList(remoteAddresses);
		}

		if (!listenerAddresses.empty()) {
			for (auto a : NetworkAddress::parseList(listenerAddresses)) {
				listeners.push_back(INetworkConnections::net()->listen(a));
			}
		}
	}

	NetworkAddress randomRemote() { return remotes[nondeterministicRandom()->randomInt(0, remotes.size())]; }

	ACTOR static Future<Standalone<StringRef>> readMsg(P2PNetworkTest* self, Reference<IConnection> conn) {
		state Standalone<StringRef> buffer = makeString(sizeof(int));
		state int writeOffset = 0;
		state bool gotHeader = false;

		// Fill buffer sequentially until the initial bytesToRead is read (or more), then read
		// intended message size and add it to bytesToRead, continue if needed until bytesToRead is 0.
		loop {
			int stutter = self->waitReadMilliseconds.get();
			if (stutter > 0) {
				wait(delay(stutter / 1e3));
			}

			int len = conn->read((uint8_t*)buffer.begin() + writeOffset, (uint8_t*)buffer.end());
			writeOffset += len;
			self->bytesReceived += len;

			// If buffer is complete, either process it as a header or return it
			if (writeOffset == buffer.size()) {
				if (gotHeader) {
					return buffer;
				} else {
					gotHeader = true;
					int msgSize = *(int*)buffer.begin();
					if (msgSize == 0) {
						return Standalone<StringRef>();
					}
					buffer = makeString(msgSize);
					writeOffset = 0;
				}
			}

			if (len == 0) {
				wait(conn->onReadable());
				wait(delay(0, TaskPriority::ReadSocket));
			}
		}
	}

	ACTOR static Future<Void> writeMsg(P2PNetworkTest* self, Reference<IConnection> conn, StringRef msg) {
		state UnsentPacketQueue packets;
		PacketWriter writer(packets.getWriteBuffer(msg.size()), nullptr, Unversioned());
		writer.serializeBinaryItem((int)msg.size());
		writer.serializeBytes(msg);

		loop {
			int stutter = self->waitWriteMilliseconds.get();
			if (stutter > 0) {
				wait(delay(stutter / 1e3));
			}
			int sent = conn->write(packets.getUnsent(), FLOW_KNOBS->MAX_PACKET_SEND_BYTES);

			if (sent != 0) {
				self->bytesSent += sent;
				packets.sent(sent);
			}

			if (packets.empty()) {
				break;
			}

			wait(conn->onWritable());
			wait(yield(TaskPriority::WriteSocket));
		}

		return Void();
	}

	ACTOR static Future<Void> doSession(P2PNetworkTest* self, Reference<IConnection> conn, bool incoming) {
		state int numRequests;

		try {
			if (incoming) {
				wait(conn->acceptHandshake());

				// Read the number of requests for the session
				Standalone<StringRef> buf = wait(readMsg(self, conn));
				ASSERT(buf.size() == sizeof(int));
				numRequests = *(int*)buf.begin();
			} else {
				wait(conn->connectHandshake());

				// Pick the number of requests for the session and send it to remote
				numRequests = self->requests.get();
				wait(writeMsg(self, conn, StringRef((const uint8_t*)&numRequests, sizeof(int))));
			}

			while (numRequests > 0) {
				if (incoming) {
					// Wait for a request
					wait(success(readMsg(self, conn)));
					// Send a reply
					wait(writeMsg(self, conn, self->msgBuffer.substr(0, self->replyBytes.get())));
				} else {
					// Send a request
					wait(writeMsg(self, conn, self->msgBuffer.substr(0, self->requestBytes.get())));
					// Wait for a reply
					wait(success(readMsg(self, conn)));
				}

				if (--numRequests == 0) {
					break;
				}
			}

			wait(delay(self->idleMilliseconds.get() / 1e3));
			conn->close();

			if (incoming) {
				++self->sessionsIn;
			} else {
				++self->sessionsOut;
			}
		} catch (Error& e) {
			++self->sessionErrors;
			TraceEvent(SevError, incoming ? "P2PIncomingSessionError" : "P2POutgoingSessionError")
			    .error(e)
			    .detail("Remote", conn->getPeerAddress());
		}

		return Void();
	}

	ACTOR static Future<Void> outgoing(P2PNetworkTest* self) {
		loop {
			wait(delay(0, TaskPriority::WriteSocket));
			state NetworkAddress remote = self->randomRemote();

			try {
				state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(remote));
				// printf("Connected to %s\n", remote.toString().c_str());
				wait(doSession(self, conn, false));
			} catch (Error& e) {
				++self->connectErrors;
				TraceEvent(SevError, "P2POutgoingError").error(e).detail("Remote", remote);
				wait(delay(1));
			}
		}
	}

	ACTOR static Future<Void> incoming(P2PNetworkTest* self, Reference<IListener> listener) {
		state ActorCollection sessions(false);

		loop {
			wait(delay(0, TaskPriority::AcceptSocket));

			try {
				state Reference<IConnection> conn = wait(listener->accept());
				// printf("Connected from %s\n", conn->getPeerAddress().toString().c_str());
				sessions.add(doSession(self, conn, true));
			} catch (Error& e) {
				++self->acceptErrors;
				TraceEvent(SevError, "P2PIncomingError").error(e).detail("Listener", listener->getListenAddress());
			}
		}
	}

	ACTOR static Future<Void> run_impl(P2PNetworkTest* self) {
		state ActorCollection actors(false);

		self->startTime = now();

		fmt::print("{0} listeners, {1} remotes, {2} outgoing connections\n",
		           self->listeners.size(),
		           self->remotes.size(),
		           self->connectionsOut);

		for (auto n : self->remotes) {
			printf("Remote: %s\n", n.toString().c_str());
		}

		for (auto el : self->listeners) {
			printf("Listener: %s\n", el->getListenAddress().toString().c_str());
			actors.add(incoming(self, el));
		}

		printf("Request size: %s\n", self->requestBytes.toString().c_str());
		printf("Response size: %s\n", self->replyBytes.toString().c_str());
		printf("Requests per outgoing session: %s\n", self->requests.toString().c_str());
		printf("Delay before socket read: %s\n", self->waitReadMilliseconds.toString().c_str());
		printf("Delay before socket write: %s\n", self->waitWriteMilliseconds.toString().c_str());
		printf("Delay before session close: %s\n", self->idleMilliseconds.toString().c_str());
		printf("Send/Recv size %d bytes\n", FLOW_KNOBS->MAX_PACKET_SEND_BYTES);

		if ((self->remotes.empty() || self->connectionsOut == 0) && self->listeners.empty()) {
			printf("No listeners and no remotes or connectionsOut, so there is nothing to do!\n");
			ASSERT((!self->remotes.empty() && (self->connectionsOut > 0)) || !self->listeners.empty());
		}

		if (!self->remotes.empty()) {
			for (int i = 0; i < self->connectionsOut; ++i) {
				actors.add(outgoing(self));
			}
		}

		loop {
			wait(delay(1.0, TaskPriority::Max));
			printf("%s\n", self->statsString().c_str());
		}
	}

	Future<Void> run() { return run_impl(this); }
};

static Standalone<StringRef> randString(int length) {
	Standalone<StringRef> str = makeString(length);
	deterministicRandom()->randomBytes(mutateString(str), length);
	return str;
}

// Fixed message sizes, ranging up to ~8MB, matching netpipe tests.
static const std::vector<int> message_sizes = {
	1,       2,       3,       4,       6,       8,       12,      13,      16,      19,      21,      24,      27,
	29,      32,      35,      45,      48,      51,      61,      64,      67,      93,      96,      99,      125,
	128,     131,     189,     192,     195,     253,     256,     259,     381,     384,     387,     509,     512,
	515,     765,     768,     771,     1021,    1024,    1027,    1533,    1536,    1539,    2045,    2048,    2051,
	3069,    3072,    3075,    4093,    4096,    4099,    6141,    6144,    6147,    8189,    8192,    8195,    12285,
	12288,   12291,   16381,   16384,   16387,   24573,   24576,   24579,   32765,   32768,   32771,   49149,   49152,
	49155,   65533,   65536,   65539,   98301,   98304,   98307,   131069,  131072,  131075,  196605,  196608,  196611,
	262141,  262144,  262147,  393213,  393216,  393219,  524285,  524288,  524291,  786429,  786432,  786435,  1048573,
	1048576, 1048579, 1572861, 1572864, 1572867, 2097149, 2097152, 2097155, 3145725, 3145728, 3145731, 4194301, 4194304,
	4194307, 6291453, 6291456, 6291459, 8388605, 8388608, 8388611,
};

class PingPongTest {
private:
	// parameters
	std::string listenAddress; // server only
	std::string remoteAddress; // client only
	bool streaming; // streaming mode (as opposed to ping-pong)

	// Shoot for each trial to be approximately this long.
	const double target_time = 0.25;

	// Internal state.
	Reference<IListener> listener;
	Reference<IConnection> conn;

public:
	PingPongTest(std::string listenAddress, std::string remoteAddress, bool streaming)
	  : listenAddress(listenAddress),
	    remoteAddress(remoteAddress),
	    streaming(streaming) {}

private:
	static Standalone<StringRef> randString(int length) {
		Standalone<StringRef> str = makeString(length);
		deterministicRandom()->randomBytes(mutateString(str), length);
		return str;
	}

	ACTOR static Future<Void> send(PingPongTest* self, const void* buf, int size) {
		state UnsentPacketQueue packets;
		PacketWriter writer(packets.getWriteBuffer(size), /* reliable */ nullptr, Unversioned());
		writer.serializeBytes(buf, size);
		loop {
			int sent = self->conn->write(packets.getUnsent(), FLOW_KNOBS->MAX_PACKET_SEND_BYTES);
			ASSERT(sent >= 0);
			if (sent > 0) {
				packets.sent(sent);
			}
			if (packets.empty()) {
				break;
			}

			if (sent == 0) {
				wait(self->conn->onWritable());
				wait(yield(TaskPriority::WriteSocket));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> receive(PingPongTest* self, void* buf, int size) {
		state int offset = 0;
		loop {
			int received = self->conn->read((uint8_t*)buf + offset, (uint8_t*)buf + size);
			ASSERT(received >= 0);
			offset += received;
			if (offset == size) {
				break;
			}

			if (received == 0) {
				wait(self->conn->onReadable());
				wait(delay(0, TaskPriority::ReadSocket));
			}
		}
		return Void();
	}

	// Before each run, the client syncs with the server, letting them know
	// how many messages to expect along with their size.
	ACTOR static Future<Void> client_handshake(PingPongTest* self, int nmessages, int message_size) {
		// Protocol:
		//   send (nmessages, message_size) to server
		//   recv ("ok") from server
		wait(send(self, &nmessages, sizeof(int)));
		wait(send(self, &message_size, sizeof(int)));

		state char* ok = new char[3];
		wait(receive(self, ok, 2));
		ASSERT(ok[0] == 'o' && ok[1] == 'k');
		delete[] ok;

		return Void();
	}

	// This is the server portion of the client/server handshake.
	ACTOR static Future<Void> server_handshake(PingPongTest* self, int* nmessages, int* message_size) {
		// Protocol:
		//   recv (nmessages, message_size) from client
		//   send ("ok") to client
		wait(receive(self, nmessages, sizeof(int)));
		wait(receive(self, message_size, sizeof(int)));
		wait(send(self, "ok", 2));

		return Void();
	}

	// The client sends/receives nmessages of size message_size.
	// It returns the estimated one-way latency.
	ACTOR static Future<double> client(PingPongTest* self, int nmessages, int message_size) {
		wait(client_handshake(self, nmessages, message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);
		state Standalone<StringRef> recvBuf = makeString(message_size);

		state double before = timer_monotonic();
		state int i;
		for (i = 0; i < nmessages; i++) {
			wait(send(self, sendBuf.begin(), sendBuf.size()));
			wait(receive(self, (void*)recvBuf.begin(), recvBuf.size()));
		}
		double after = timer_monotonic();

		// Estimate one-way latency as half of a round trip.
		return (after - before) / (2 * nmessages);
	}

	// The server bounces messages back to the client.
	ACTOR static Future<Void> server(PingPongTest* self) {
		state int nmessages;
		state int message_size;
		wait(server_handshake(self, &nmessages, &message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);
		state Standalone<StringRef> recvBuf = makeString(message_size);

		// Test loop.
		state int i;
		for (i = 0; i < nmessages; i++) {
			wait(receive(self, (void*)recvBuf.begin(), recvBuf.size()));
			wait(send(self, sendBuf.begin(), sendBuf.size()));
		}

		return Void();
	}

	// The client sends a stream of nmessages of size message_size.
	// It returns the estimated one-way latency.
	ACTOR static Future<double> client_streaming(PingPongTest* self, int nmessages, int message_size) {
		wait(client_handshake(self, nmessages, message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);

		state int i;
		for (i = 0; i < nmessages; i++) {
			wait(send(self, sendBuf.begin(), sendBuf.size()));
		}

		// To avoid just measuring how long it takes to buffer,
		// let server compute latency.
		state double latency;
		wait(receive(self, &latency, sizeof(double)));
		return latency;
	}

	// The server receives a stream of messages from a client,
	// and computes the latency to receive each message.
	ACTOR static Future<Void> server_streaming(PingPongTest* self) {
		state int nmessages;
		state int message_size;
		wait(server_handshake(self, &nmessages, &message_size));

		state Standalone<StringRef> recvBuf = makeString(message_size);

		// Test loop.
		state double before = timer_monotonic();
		state int i;
		for (i = 0; i < nmessages; i++) {
			wait(receive(self, (void*)recvBuf.begin(), recvBuf.size()));
		}
		double after = timer_monotonic();

		state double latency = (after - before) / nmessages;
		wait(send(self, &latency, sizeof(double)));
		return Void();
	}

	ACTOR static Future<Void> do_run(PingPongTest* self) {
		state bool sending = false;
		state bool receiving = false;

		// Setup.
		if (!self->listenAddress.empty()) {
			NetworkAddress addr = NetworkAddress::parse(self->listenAddress);
			self->listener = INetworkConnections::net()->listen(addr);
			Reference<IConnection> conn = wait(self->listener->accept());
			self->conn = conn;
			wait(self->conn->acceptHandshake());
			receiving = true;
		}
		if (!self->remoteAddress.empty()) {
			NetworkAddress addr = NetworkAddress::parse(self->remoteAddress);
			Reference<IConnection> conn = wait(INetworkConnections::net()->connect(addr));
			self->conn = conn;
			wait(self->conn->connectHandshake());
			sending = true;
		}

		// Sanity check. Send or receive, not both.
		ASSERT(sending ^ receiving);

		// Bootstrap one-way ping latency.
		state double lastLatency;
		if (sending) {
			double latency = wait(client(self, 100, 1));
			lastLatency = latency;
		} else {
			wait(server(self));
		}

		state int i;
		for (i = 0; i < message_sizes.size(); i++) {
			state int message_size = message_sizes[i];

			// Choose the number of messages to hit target
			// time, using one-way latency from the last
			// round as an estimate of what latency will
			// be for this round.
			state int nmessages = ::ceil(self->target_time / (lastLatency * 2));
			if (sending) {
				::printf("%3d: %7d bytes %6d times --> ", i, message_size, nmessages);

				double latency = wait(self->streaming ? client_streaming(self, nmessages, message_size)
				                                      : client(self, nmessages, message_size));

				::printf(" %8.2lf MBps @ %10.3lf usec\n", message_size / (latency * 1024 * 1024), latency * 1e6);

				// easy to plot format
				::fprintf(stderr, "%8d %12.9lf\n", message_size, latency);

				lastLatency = latency;
			} else {
				wait(self->streaming ? server_streaming(self) : server(self));
			}
		}

		return Void();
	}

public:
	Future<Void> run() { return do_run(this); }
};

class FlowTransportTest {
private:
	// parameters
	std::string listenAddress; // server only
	std::string remoteAddress; // client only
	bool streaming; // streaming mode (as opposed to ping-pong)

	// Shoot for each trial to be approximately this long.
	const double target_time = 0.25;

	// Internal state.
	NetworkTestInterface interf;
	// Reference<IListener> listener;
	// Reference<IConnection> conn;

public:
	FlowTransportTest(std::string listenAddress, std::string remoteAddress, bool streaming)
	  : listenAddress(listenAddress),
	    remoteAddress(remoteAddress),
	    streaming(streaming) {}

private:
	// Before each run, the client syncs with the server, letting them know
	// how many messages to expect along with their size.
	ACTOR static Future<Void> client_handshake(FlowTransportTest* self, int nmessages, int message_size) {
		// Protocol:
		//   send (nmessages, message_size) to server
		//   recv ("ok") from server
		wait(self->interf.start.getReply(NetworkTestStartRequest(nmessages, message_size)));

		return Void();
	}

	// This is the server portion of the client/server handshake.
	ACTOR static Future<Void> server_handshake(FlowTransportTest* self, int* nmessages, int* message_size) {
		// Protocol:
		//   recv (nmessages, message_size) from client
		//   send ("ok") to client
		NetworkTestStartRequest req = waitNext(self->interf.start.getFuture());
		*nmessages = req.nmessages;
		*message_size = req.message_size;
		req.reply.send(Void());

		return Void();
	}

	// The client sends/receives nmessages of size message_size.
	// It returns the estimated one-way latency.
	ACTOR static Future<double> client(FlowTransportTest* self, int nmessages, int message_size) {
		wait(client_handshake(self, nmessages, message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);

		state double before = timer_monotonic();
		state int i;
		for (i = 0; i < nmessages; i++) {
			NetworkTestReply reply = wait(self->interf.test.getReply(NetworkTestRequest(sendBuf, message_size)));
		}
		double after = timer_monotonic();

		// Estimate one-way latency as half of a round trip.
		return (after - before) / (2 * nmessages);
	}

	// The server bounces messages back to the client.
	ACTOR static Future<Void> server(FlowTransportTest* self) {
		state int nmessages;
		state int message_size;
		wait(server_handshake(self, &nmessages, &message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);

		::printf("%7d bytes %6d times --> ", message_size, nmessages);
		// Test loop.
		state int i;
		for (i = 0; i < nmessages; i++) {
			NetworkTestRequest request = waitNext(self->interf.test.getFuture());
			ASSERT(request.replySize == message_size);
			request.reply.send(NetworkTestReply(Value(sendBuf)));
		}

		return Void();
	}

	// The client asks the server to send a stream of nmessages of size message_size.
	// It returns the estimated one-way latency.
	ACTOR static Future<double> client_streaming(FlowTransportTest* self, int nmessages, int message_size) {
		wait(client_handshake(self, nmessages, message_size));

		NetworkTestStreamingRequest request;
		state ReplyPromiseStream<NetworkTestStreamingReply> stream = self->interf.testStream.getReplyStream(request);

		// Test loop.
		state double before = timer_monotonic();
		state int i;
		for (i = 0; i < nmessages; i++) {
			NetworkTestStreamingReply reply = waitNext(stream.getFuture());
			ASSERT(reply.index == i);
		}
		double after = timer_monotonic();

		// average latency
		return (after - before) / nmessages;
	}

	// The server sends a stream of messages to the client.
	ACTOR static Future<Void> server_streaming(FlowTransportTest* self) {
		state int nmessages;
		state int message_size;
		wait(server_handshake(self, &nmessages, &message_size));

		state Standalone<StringRef> sendBuf = randString(message_size);
		state NetworkTestStreamingRequest request = waitNext(self->interf.testStream.getFuture());

		state int i;
		for (i = 0; i < nmessages; i++) {
			request.reply.send(NetworkTestStreamingReply{ i, Value(sendBuf) });
		}

		return Void();
	}

	ACTOR static Future<Void> do_run(FlowTransportTest* self) {
		state bool sending = false;
		state bool receiving = false;

		// Setup.
		if (!self->listenAddress.empty()) {
			NetworkAddress addr = NetworkAddress::parse(self->listenAddress);
			::printf("Binding %s\n", self->listenAddress.c_str());
			try {
				const Future<Void>& errorF = FlowTransport::transport().bind(addr, addr);
				if (errorF.isReady())
					errorF.get();
			} catch (Error& e) {
				fprintf(stderr, "Binding %s failed (%s)\n", self->listenAddress.c_str(), e.what());
				flushAndExit(FDB_EXIT_ERROR);
			}
			self->interf = NetworkTestInterface(g_network);
			receiving = true;
		}
		if (!self->remoteAddress.empty()) {
			NetworkAddress addr = NetworkAddress::parse(self->remoteAddress);
			self->interf = NetworkTestInterface(addr);
			sending = true;
		}

		// Sanity check. Send or receive, not both.
		ASSERT(sending ^ receiving);

		// Bootstrap one-way ping latency.
		::printf("Bootstrapping ping latency.\n");
		state double lastLatency;
		if (sending) {
			double latency = wait(client(self, 100, 1));
			lastLatency = latency;
			::printf("Estimated latency = %10.3lf usec\n", lastLatency * 1e6);
		} else {
			wait(server(self));
		}

		state int i;
		for (i = 0; i < message_sizes.size(); i++) {
			state int message_size = message_sizes[i];

			// Choose the number of messages to hit target
			// time, using one-way latency from the last
			// round as an estimate of what latency will
			// be for this round.
			if (sending) {
				int nmessages = ::ceil(self->target_time / (lastLatency * 2));
				::printf("%3d: %7d bytes %6d times --> ", i, message_size, nmessages);

				double latency = wait(self->streaming ? client_streaming(self, nmessages, message_size)
				                                      : client(self, nmessages, message_size));

				::printf(" %8.2lf MBps @ %10.3lf usec\n", message_size / (latency * 1024 * 1024), latency * 1e6);

				// easy to plot format
				::fprintf(stderr, "%8d %12.9lf\n", message_size, latency);

				lastLatency = latency;
			} else {
				::printf("%3d: ", i);
				wait(self->streaming ? server_streaming(self) : server(self));
				::printf("DONE\n");
			}
		}

		if (receiving) {
			// BUG: The last server response won't
			// actually get sent unless we introduce this
			// artifical wait for a request that's never
			// coming. It will timeout, and hopefully
			// connectionWriter will have done its thing
			// and actually sent the response so the
			// client can finish. delay() is not
			// sufficient to get connectionWriter to
			// flush.
			NetworkTestStartRequest req = waitNext(self->interf.start.getFuture());
		}

		return Void();
	}

public:
	Future<Void> run() { return do_run(this); }
};

// Peer-to-Peer network test.
// One or more instances can be run and set to talk to each other.
// Each instance
//   - listens on 0 or more listenerAddresses
//   - maintains 0 or more connectionsOut at a time, each to a random choice from remoteAddresses
// Address lists are a string of comma-separated IP:port[:tls] strings.
//
// The other arguments can be specified as "fixedValue" or "minValue:maxValue".
// Each outgoing connection will live for a random requests count.
// Each request will
//   - send a random requestBytes sized message
//   - wait for a random replyBytes sized response.
// The client will close the connection after a random idleMilliseconds.
// Reads and writes can optionally preceded by random delays, waitReadMilliseconds and waitWriteMilliseconds.
TEST_CASE(":/network/p2ptest") {
	state P2PNetworkTest p2p(params.get("listenerAddresses").orDefault(""),
	                         params.get("remoteAddresses").orDefault(""),
	                         params.getInt("connectionsOut").orDefault(1),
	                         params.get("requestBytes").orDefault("50:100"),
	                         params.get("replyBytes").orDefault("500:1000"),
	                         params.get("requests").orDefault("10:10000"),
	                         params.get("idleMilliseconds").orDefault("0"),
	                         params.get("waitReadMilliseconds").orDefault("0"),
	                         params.get("waitWriteMilliseconds").orDefault("0"));

	wait(p2p.run());
	return Void();
}

// Ping-pong network test.
//
// Bounces one packet back and forth between client and server.
//
// Start server with
//    bin/fdbserver -r unittests -f :/network/pingpongtest --test-listen <addr>
// then client with
//    bin/fdbserver -r unittests -f :/network/pingpongtest --test-remote <addr>
//
// The test will automatically vary the message size and choose an appropriate number
// of trials. The output will contain message size, throughput, and average latency.
TEST_CASE(":/network/pingpongtest") {
	state PingPongTest pp(params.get("listen").orDefault(""), params.get("remote").orDefault(""), false);
	wait(pp.run());
	return Void();
}

// Streaming network test.
//
// Sends a stream of packets between client and server.
//
// Start server with
//    bin/fdbserver -r unittests -f :/network/streamingtest --test-listen <addr>
// then client with
//    bin/fdbserver -r unittests -f :/network/streamingtest --test-remote <addr>
//
// The test will automatically vary the message size and choose an appropriate number
// of trials. The output will contain message size, throughput, and average latency.
TEST_CASE(":/network/streamingtest") {
	state PingPongTest pp(params.get("listen").orDefault(""), params.get("remote").orDefault(""), true);
	wait(pp.run());
	return Void();
}

// FlowTransport ping-long network test.
//
// Same as the ping-pong test, but with FlowTransport involved.
//
// Start server with
//    bin/fdbserver -r unittests -f :/network/flowtransport/pingpongtest --test-listen <addr>
// then client with
//    bin/fdbserver -r unittests -f :/network/flowtransport/pingpongtest --test-remote <addr>
TEST_CASE(":/network/flowtransport/pingpongtest") {
	state FlowTransportTest pp(params.get("listen").orDefault(""), params.get("remote").orDefault(""), false);
	wait(pp.run());
	return Void();
}

// FlowTransport streaming network test.
//
// Same as the streaming test, but with FlowTransport involved.
//
// Start server with
//    bin/fdbserver -r unittests -f :/network/flowtransport/streamingtest --test-listen <addr>
// then client with
//    bin/fdbserver -r unittests -f :/network/flowtransport/streamingtest --test-remote <addr>
TEST_CASE(":/network/flowtransport/streamingtest") {
	state FlowTransportTest pp(params.get("listen").orDefault(""), params.get("remote").orDefault(""), true);
	wait(pp.run());
	return Void();
}
