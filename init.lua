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

lmdb.deserialize =  function(val, sz, binary)
   if binary then
      local ret = torch.ByteTensor(sz)
       ffi.copy(ret:data(), val, sz)
       return ret
   end
    local str = ffi.string(val, sz)
    local ok,ret = pcall(torch.deserialize, str)
    if not ok then
       ret = torch.ByteTensor(sz)
       ffi.copy(ret:data(), val, sz)
    end
    return ret
end

local errcheck = function(f, ...)
    local status = C[f](...)
    if status and status ~= C.MDB_SUCCESS and lmdb.verbose then
        print("Error in LMDB function " .. f .. " : ", ffi.string(C.mdb_strerror(status)))
    end
    return status
end

lmdb.errcheck = errcheck

lmdb.MDB_val = function(mdb_val, x, is_key) --key will always be turned to string
    local mdb_val = mdb_val or ffi.new('MDB_val[1]')
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

lmdb.from_MDB_val = function(mdb_val, is_key, binary)
    local sz = tonumber(mdb_val[0].mv_size)
    local data = mdb_val[0].mv_data
    if is_key then
        return ffi.string(data, sz)
    else
        return lmdb.deserialize(data, sz, binary)
    end
end


return lmdb
