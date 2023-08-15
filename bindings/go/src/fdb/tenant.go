package fdb

// #define FDB_API_VERSION 900000
// #include <foundationdb/fdb_c.h>
import "C"

type tenant struct {
	ptr *C.FDBTenant
}

type TenantOptions struct {
    t *tenant
}

func (opt TenantOptions) setOpt(code int, param []byte) error {
	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_tenant_set_option(opt.t.ptr, C.FDBTenantOption(code), p, pl)
	}, param)
}
