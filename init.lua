lmdb = {}
require 'torch'
include('ffi.lua')
include('DB.lua')

local ffi = require 'ffi'
local C = lmdb.C
lmdb.serialize = torch.serialize
lmdb.deserialize = torch.deserialize

local errcheck = function(f, ...)
    local status = C[f](...)
    if status ~= C.MDB_SUCCESS then
        print("Error in LMDB: ", ffi.string(C.mdb_strerror(tonumber(status))))
    end
end

lmdb.errcheck = errcheck

lmdb.MDB_val = function(x)
    local mdb_val = ffi.new('MDB_val[1]')
    local value = lmdb.serialize(x)

    mdb_val[0].mv_size = #value
    mdb_val[0].mv_data = ffi.cast('void*', value)

    return mdb_val
end

lmdb.from_MDB_val = function(mdb_val)
    --local x = torch.Tensor()
    --ffi.copy(x:cdata(), mdb_val[0].mv_data, tonumber(mdb_val[0].mv_size))
    local str = ffi.string(mdb_val[0].mv_data, tonumber(mdb_val[0].mv_size))
    local x = lmdb.deserialize(str)
    return x
end


return lmdb
