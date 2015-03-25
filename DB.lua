require 'xlua'
require 'dok'
local C = lmdb.C
local ffi = require 'ffi'



---------------------------------------------env-------------------------------------------
local env = torch.class('lmdb.env')

function env:__init()
    self.mdb_env = ffi.new('MDB_env *[1]')
    lmdb.errcheck('mdb_env_create',self.mdb_env)
    local function destroy_env(x)
        self:close()
    end
--    self.mdb_env = self.mdb_env[0]
    ffi.gc(self.mdb_env, destroy_env )
end

function env:open(...)
    local args = dok.unpack(
    {...},
    'DB:open',
    'Initializes a LMDB Database',
    {arg='Path', type='string', help='Name of DataProvider',req = true},
    {arg='MapSize', type='number', help='Size of map' , default = 1099511627776},
    {arg='NOSUBDIR', type='boolean', help='', default = false},
    {arg='NOMETASYNC', type='boolean', help='', default = false},
    {arg='RDONLY', type='boolean', help='', default = false},
    {arg='WRITEMAP', type='boolean', help='', default = false},
    {arg='MAPASYNC', type='boolean', help='', default = false},
    {arg='NOSYNC', type='boolean', help='', default = false},
    {arg='NOTLS', type='boolean', help='', default = false},
    {arg='NOLOCK', type='boolean', help='', default = false},
    {arg='Mode', type='number', help='', default = 0664},
    {arg='MaxDBs', type='number', help='', default = 1},
    {arg='MaxReaders', type='number', help='', default = 3},
    {arg='Name', type='string', help='', default = 'Data'}
    )

    local flags = 0
    if args.NOTLS then flags = bit.bor(flags, C.MDB_NOTLS) end
    if args.NOSUBDIR then flags = bit.bor(flags, C.MDB_NOSUBDIR) end
    if args.NOMETASYNC then flags = bit.bor(flags, C.MDB_NOMETASYNC) end
    if args.RDONLY then flags = bit.bor(flags, C.MDB_RDONLY) end
    if args.WRITEMAP then flags = bit.bor(flags, C.MDB_WRITEMAP) end
    if args.MAPASYNC then flags = bit.bor(flags, C.MDB_MAPASYNC) end
    if args.NOSYNC then flags = bit.bor(flags, C.MDB_NOSYNC) end
    if args.NOLOCK then flags = bit.bor(flags, C.MDB_NOLOCK) end
    self.Name = args.Name
    lmdb.errcheck('mdb_env_set_mapsize',self.mdb_env[0], args.MapSize) 
    lmdb.errcheck('mdb_env_set_maxdbs',self.mdb_env[0], args.MaxDBs) 
    lmdb.errcheck('mdb_env_set_maxreaders',self.mdb_env[0], args.MaxReaders) 
    local path = paths.concat(args.Path)
    os.execute('mkdir -p "' .. path .. '"')
    lmdb.errcheck('mdb_env_open',self.mdb_env[0],path, flags, tonumber(args.Mode,8))
end

function env:txn(...)
    return lmdb.txn(self,...)
end

function env:reader_check()
    local num = ffi.new('int [1]')
    lmdb.errcheck('mdb_reader_check', self.mdb_env[0], num)
    return tonumber(num[0])
end

function env:stat()
    if not self.mdb_stat then
        self.mdb_stat= ffi.new('MDB_stat [1]')
    end
    lmdb.errcheck('mdb_env_stat', self.mdb_env[0], self.mdb_stat)
    return {
        psize = tonumber(self.mdb_stat[0].ms_psize),
        depth = tonumber(self.mdb_stat[0].ms_depth),
        branch_pages = tonumber(self.mdb_stat[0].ms_branch_pages),
        leaf_pages = tonumber(self.mdb_stat[0].ms_leaf_pages),
        overflow_pages = tonumber(self.mdb_stat[0].ms_overflow_pages),
        entries = tonumber(self.mdb_stat[0].ms_entries),

}
end


function env:close()
    lmdb.errcheck('mdb_env_close', self.mdb_env[0])
end












---------------------------------------------txn-------------------------------------------
local txn = torch.class('lmdb.txn')

function txn:__init(env_obj, rdonly, parent_txn)
    self.mdb_txn = ffi.new('MDB_txn *[1]')
    local function destroy_txn(x)
        self:dbi_close()
        self:abort()
    end
    ffi.gc(self.mdb_txn, destroy_txn )
    local parent = nil
    if parent_txn then
        parent = parent_txn.mdb_txn[0]
    end

    local flag = 0
    if rdonly then
        flag = lmdb.C.MDB_RDONLY
    end
    lmdb.errcheck('mdb_txn_begin',env_obj.mdb_env[0], parent, flag, self.mdb_txn)
    self:dbi_open()
end

function txn:dbi_open(name, flags)
    self.mdb_dbi = ffi.new('MDB_dbi[1]')
    local flags = flags or 0
    return lmdb.errcheck('mdb_dbi_open', self.mdb_txn[0], name, flags, self.mdb_dbi)
end

function txn:dbi_close()
    return lmdb.errcheck('mdb_dbi_close', self.mdb_dbi[0])
end

function txn:commit()
    return lmdb.errcheck('mdb_txn_commit', self.mdb_txn[0])
end

function txn:abort()
    return lmdb.errcheck('mdb_txn_abort', self.mdb_txn[0])
end

function txn:reset()
    return lmdb.errcheck('mdb_txn_reset', self.mdb_txn[0])
end

function txn:renew()
    return lmdb.errcheck('mdb_txn_renew', self.mdb_txn[0])
end



function txn:put(key, data, flag)
    local flag = flag or 0
    local mdb_key = lmdb.MDB_val(key)
    local mdb_data = lmdb.MDB_val(data)
    return lmdb.errcheck('mdb_put', self.mdb_txn[0], self.mdb_dbi[0], mdb_key,mdb_data, flag)
end

function txn:cursor()
    return lmdb.cursor(self)
end

function txn:get(key)
    local mdb_key = lmdb.MDB_val(key)
    local mdb_data = ffi.new('MDB_val[1]')
    if lmdb.errcheck('mdb_get', self.mdb_txn[0], self.mdb_dbi[0], mdb_key,mdb_data) == nil then
        return nil
    else
        return lmdb.from_MDB_val(mdb_data)
    end
end

---------------------------------------------txn-------------------------------------------
local cursor = torch.class('lmdb.cursor')

function cursor:__init(txn_obj)
    self.mdb_cursor = ffi.new('MDB_cursor *[1]')

    local function destroy_cursor(x)
        self:close()
    end
    ffi.gc(self.mdb_cursor, destroy_cursor )

    return lmdb.errcheck('mdb_cursor_open',txn_obj.mdb_txn[0], txn_obj.mdb_dbi[0], self.mdb_cursor)
end

function cursor:get(op)
    local op = op or lmdb.C.MDB_NEXT
    local mdb_key = ffi.new('MDB_val[1]')
    local mdb_data = ffi.new('MDB_val[1]')

    if lmdb.errcheck('mdb_cursor_get', self.mdb_cursor[0], mdb_key, mdb_data, op) == nil then
        return nil
    else
        return lmdb.from_MDB_val(mdb_data)
    end
end

function cursor:put(key, data, flag)
    local flag = flag or 0
    local mdb_key = lmdb.MDB_val(key)
    local mdb_data = lmdb.MDB_val(data)
    return lmdb.errcheck('mdb_cursor_put', self.mdb_cursor[0], mdb_key,mdb_data, flag)
end

function cursor:del(flag)
    local flag = flag or 0
    return lmdb.errcheck('mdb_cursor_del', self.mdb_cursor[0], flag)
end

function cursor:close()
    return lmdb.errcheck('mdb_cursor_close', self.mdb_cursor[0])
end




