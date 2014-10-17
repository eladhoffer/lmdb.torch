lmdb = {}

include 'ffi.lua'

local thrift = require 'fb.thrift'
local C = lmdb.C
local ffi = require 'ffi'

local errcheck = function(f, ...)
    local status = C[f](...)
    if status ~= C.MDB_SUCCESS then
        print("Error in LMDB: ", ffi.string(C.mdb_strerror(tonumber(status))))
    end
end
lmdb.errcheck = errcheck

lmdb.MDB_val = function(x)
    local mdb_val = ffi.new('MDB_val[1]')
    local value
    if type(x) == 'number' then
        value = tostring(x)
        --     value = ffi.new('int32_t[1]',x)
        --     mdb_val[0].mv_size = ffi.sizeof(value)
    elseif type(x) == 'string' then
        value = x
    else
        value = thrift.to_string(x)
    end

    mdb_val[0].mv_size = #value
    mdb_val[0].mv_data = ffi.cast('void*', value)

    return mdb_val
end

lmdb.from_MDB_val = function(mdb_val)
    --local x = torch.Tensor()
    --ffi.copy(x:cdata(), mdb_val[0].mv_data, tonumber(mdb_val[0].mv_size))
    local str = ffi.string(mdb_val[0].mv_data, tonumber(mdb_val[0].mv_size))
    local x = thrift.from_string(str)
    return x
end

include 'DB.lua'


