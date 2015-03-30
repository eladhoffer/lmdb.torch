lmdb = {}
require 'torch'
include('ffi.lua')
include('DB.lua')

local ffi = require 'ffi'
local C = lmdb.C

lmdb.verbose = true

lmdb.serialize = function(x)
    local val = torch.serialize(x)
    local sz = #val
    return val, sz
end

lmdb.deserialize =  function(val, sz)
    local str = ffi.string(val, sz)
    return torch.deserialize(str)
end

local errcheck = function(f, ...)
    local status = C[f](...)
    if status and status ~= C.MDB_SUCCESS and lmdb.verbose then
        print("Error in LMDB: ", ffi.string(C.mdb_strerror(status)))
        return nil
    end
    return status
end

lmdb.errcheck = errcheck

lmdb.MDB_val = function(x, is_key) --key will always be turned to string
    local mdb_val = ffi.new('MDB_val[1]')
    local value
    if is_key then
        value = tostring(x)
        mdb_val[0].mv_size = #value
    else
        value, mdb_val[0].mv_size = lmdb.serialize(x)
    end
    mdb_val[0].mv_data = ffi.cast('void*', value)
    return mdb_val
end

lmdb.from_MDB_val = function(mdb_val, is_key)
    local sz = tonumber(mdb_val[0].mv_size)
    local data = mdb_val[0].mv_data
    if is_key then
        return ffi.string(data, sz)
    else
        return lmdb.deserialize(data, sz)
    end
end


return lmdb
