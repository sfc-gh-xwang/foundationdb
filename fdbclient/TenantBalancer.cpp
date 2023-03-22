#include "fdbclient/TenantBalancer.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/TenantBalancerInterface.h"

const Key TenantBalancerMetadata::tenantBalancerInterfKey("\xff/TenantBalancerInterf"_sr);

KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())>
TenantBalancerMetadata::getTenantBalancerInterface() {
	static KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())> tenantBalancerKeyBackedProperty(
	    tenantBalancerInterfKey, IncludeVersion());
	return tenantBalancerKeyBackedProperty;
}