/*
 * IClusterConnectionRecord.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_CLUSTERCONNECTIONRECORD_H
#define FDBCLIENT_CLUSTERCONNECTIONRECORD_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/Hostname.h"

// A string containing the information necessary to connect to a cluster.
//
// The format of the connection string is: description:id@[addrs]+
// The description and id together are called the "key"
//
// The following is enforced about the format of the file:
//  - The key must contain one (and only one) ':' character
//  - The description contains only allowed characters (a-z, A-Z, 0-9, _)
//  - The ID contains only allowed characters (a-z, A-Z, 0-9)
//  - At least one address is specified
//  - There is no address present more than once
class ClusterConnectionString {
public:
	constexpr static FileIdentifier file_identifier = 13602011;

	ClusterConnectionString() {}
	ClusterConnectionString(const std::string& connectionString);
	ClusterConnectionString(const std::vector<NetworkAddress>& coordinators, Key key);
	ClusterConnectionString(const std::vector<Hostname>& hosts, Key key);

	Key clusterKey() const { return key; }
	Key clusterKeyName() const {
		return keyDesc;
	} // Returns the "name" or "description" part of the clusterKey (the part before the ':')
	std::string toString() const;
	static std::string getErrorString(std::string const& source, Error const& e);

	void parseKey(const std::string& key);

	// This function tries to resolve all hostnames once, and return them with coords.
	// Best effort, does not guarantee that the resolves succeed.
	Future<std::vector<NetworkAddress>> tryResolveHostnames();

	std::vector<NetworkAddress> coords;
	std::vector<Hostname> hostnames;

	size_t getNumberOfCoordinators() const { return coords.size() + hostnames.size(); }

	// Determine the local source IP used to connect to the cluster by connecting to the first available coordinator.
	// Throw bind_failed() if no connection attempts were successful.
	// This function blocks on connection attempts.
	IPAddress determineLocalSourceIP() const;

	bool operator==(const ClusterConnectionString& other) const noexcept {
		return key == other.key && keyDesc == other.keyDesc && coords == other.coords && hostnames == other.hostnames;
	}
	bool operator!=(const ClusterConnectionString& other) const noexcept { return !(*this == other); }

private:
	void parseConnString();
	Key key, keyDesc;

public:
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, coords, hostnames, key, keyDesc);
	}
};

FDB_BOOLEAN_PARAM(ConnectionStringNeedsPersisted);

// A record that stores the connection string used to connect to a cluster. This record can be updated when a cluster
// notifies a connected party that the connection string has changed.
//
// The typically used cluster connection record is a cluster file (implemented in ClusterConnectionFile). This interface
// provides an abstraction over the cluster file so that we can persist the connection string in other locations or have
// one that is only stored in memory.
class IClusterConnectionRecord {
public:
	IClusterConnectionRecord(ConnectionStringNeedsPersisted connectionStringNeedsPersisted)
	  : connectionStringNeedsPersisted(connectionStringNeedsPersisted) {}
	virtual ~IClusterConnectionRecord() {}

	// Returns the connection string currently held in this object. This may not match the stored record if it hasn't
	// been persisted or if the persistent storage for the record has been modified externally.
	ClusterConnectionString& getConnectionString();

	// Sets the connections string held by this object and persists it.
	virtual Future<Void> setAndPersistConnectionString(ClusterConnectionString const&) = 0;

	// If this record is backed by persistent storage, get the connection string from that storage. Otherwise, return
	// the connection string stored in memory.
	virtual Future<ClusterConnectionString> getStoredConnectionString() = 0;

	// Checks whether the connection string in persisten storage matches the connection string stored in memory.
	Future<bool> upToDate();

	// Checks whether the connection string in persisten storage matches the connection string stored in memory. The
	// cluster string stored in persistent storage is returned via the reference parameter connectionString.
	virtual Future<bool> upToDate(ClusterConnectionString& connectionString) = 0;

	// Returns a string representing the location of the cluster record. For example, this could be the filename or key
	// that stores the connection string.
	virtual std::string getLocation() const = 0;

	// Creates a copy of this object with a modified connection string but that isn't persisted.
	virtual Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const = 0;

	// Returns a string representation of this cluster connection record. This will include the type and location of the
	// record.
	virtual std::string toString() const = 0;

	// Signals to the connection record that it was successfully used to connect to a cluster.
	void notifyConnected();

	virtual void addref() = 0;
	virtual void delref() = 0;

protected:
	// Writes the connection string to the backing persistent storage, if applicable.
	virtual Future<bool> persist() = 0;

	// Returns whether the connection record contains a connection string that needs to be persisted upon connection.
	bool needsToBePersisted() const;

	// Clears the flag needs persisted flag.
	void setPersisted();

	ClusterConnectionString cs;

private:
	// A flag that indicates whether this connection record needs to be persisted when it succesfully establishes a
	// connection.
	bool connectionStringNeedsPersisted;
};

template <>
struct Traceable<IClusterConnectionRecord> : std::true_type {
	static std::string toString(IClusterConnectionRecord const& record) { return record.toString(); }
};

#endif