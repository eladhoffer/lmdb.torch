require 'xlua'
require 'dok'
local C = lmdb.C
local ffi = require 'ffi'



---------------------------------------------env-------------------------------------------
local env = torch.class('lmdb.env')

function env:__init(...)
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

    self.flags = flags
    self.Path = args.Path
    self.Name = args.Name
    self.Mode = args.Mode
    self.MapSize = args.MapSize
    self.MaxDBs = args.MaxDBs
    self.MaxReaders = args.MaxReaders

end

function env:open()
    self.mdb_env = ffi.new('MDB_env *[1]')
    lmdb.errcheck('mdb_env_create',self.mdb_env)
    local function destroy_env(x)
        self:close()
    end
    --    self.mdb_env = self.mdb_env[0]
    ffi.gc(self.mdb_env, destroy_env )

    lmdb.errcheck('mdb_env_set_mapsize',self.mdb_env[0], self.MapSize) 
    lmdb.errcheck('mdb_env_set_maxdbs',self.mdb_env[0], self.MaxDBs) 
    lmdb.errcheck('mdb_env_set_maxreaders',self.mdb_env[0], self.MaxReaders) 
    local path = paths.concat(self.Path)
    os.execute('mkdir -p "' .. path .. '"')
    lmdb.errcheck('mdb_env_open',self.mdb_env[0],path, self.flags, tonumber(self.Mode,8))
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
    local stat = {
        psize = tonumber(self.mdb_stat[0].ms_psize),
        depth = tonumber(self.mdb_stat[0].ms_depth),
        branch_pages = tonumber(self.mdb_stat[0].ms_branch_pages),
        leaf_pages = tonumber(self.mdb_stat[0].ms_leaf_pages),
        overflow_pages = tonumber(self.mdb_stat[0].ms_overflow_pages),
        entries = tonumber(self.mdb_stat[0].ms_entries),

    }
    self.mdb_stat = nil
    return stat
end


function env:close()
    if self.mdb_env then
        lmdb.errcheck('mdb_env_close', self.mdb_env[0])
        self.mdb_env = nil
    end
end


---------------------------------------------txn-------------------------------------------
local txn = torch.class('lmdb.txn')

function txn:__init(env_obj, rdonly, parent_txn)
    self.mdb_txn = ffi.new('MDB_txn *[1]')
    local function destroy_txn(x)
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
    self.mdb_dbi = self.mdb_dbi or ffi.new('MDB_dbi[1]')
    local flags = flags or 0
    return lmdb.errcheck('mdb_dbi_open', self.mdb_txn[0], name, flags, self.mdb_dbi)
end

function txn:commit()
    lmdb.errcheck('mdb_txn_commit', self.mdb_txn[0])
end

function txn:abort()
    lmdb.errcheck('mdb_txn_abort', self.mdb_txn[0])
end

function txn:reset()
    lmdb.errcheck('mdb_txn_reset', self.mdb_txn[0])
end

function txn:renew()
    return lmdb.errcheck('mdb_txn_renew', self.mdb_txn[0])
end



function txn:put(key, data, flag)
    local flag = flag or 0
    self.mdb_key = lmdb.MDB_val(self.mdb_key, key, true) --Keys are always strings
    self.mdb_data = lmdb.MDB_val(self.mdb_data, data)
    return lmdb.errcheck('mdb_put', self.mdb_txn[0], self.mdb_dbi[0], self.mdb_key, self.mdb_data, flag)
end

function txn:cursor()
    return lmdb.cursor(self)
end

function txn:get(key)
    self.mdb_key = lmdb.MDB_val(self.mdb_key, key, true)
    self.mdb_data = self.mdb_data or ffi.new('MDB_val[1]')
    if lmdb.errcheck('mdb_get', self.mdb_txn[0], self.mdb_dbi[0], self.mdb_key,self.mdb_data) == lmdb.C.MDB_NOTFOUND then
        return nil
    else
        return lmdb.from_MDB_val(self.mdb_data)
    end
end

function txn:clear()
    self.mdb_key = nil
    self.mdb_data = nil
end

---------------------------------------------txn-------------------------------------------
local cursor = torch.class('lmdb.cursor')

function cursor:__init(txn_obj)
    self.mdb_cursor = ffi.new('MDB_cursor *[1]')

    local function destroy_cursor(x)
        self:close()
    end
    ffi.gc(self.mdb_cursor, destroy_cursor )

    lmdb.errcheck('mdb_cursor_open',txn_obj.mdb_txn[0], txn_obj.mdb_dbi[0], self.mdb_cursor)
    self:first()
end
function cursor:get(op)
    local op = op or lmdb.C.MDB_GET_CURRENT
    self.mdb_key = self.mdb_key or ffi.new('MDB_val[1]')
    self.mdb_data = self.mdb_key or ffi.new('MDB_val[1]')

    if lmdb.errcheck('mdb_cursor_get', self.mdb_cursor[0], self.mdb_key, self.mdb_data, op) == lmdb.C.MDB_NOTFOUND then
        return nil
    else
        return lmdb.from_MDB_val(self.mdb_key, true), lmdb.from_MDB_val(self.mdb_data)
    end
end


function cursor:set(key)
    local op  = lmdb.C.MDB_SET
    self.mdb_key = lmdb.MDB_val(self.mdb_key, key, true)

    if lmdb.errcheck('mdb_cursor_get', self.mdb_cursor[0], self.mdb_key, nil, op) == lmdb.C.MDB_NOTFOUND then
        return false
    else
        return true
    end
end

function cursor:move(op)
    local op = op or lmdb.C.MDB_GET_CURRENT

    if lmdb.errcheck('mdb_cursor_get', self.mdb_cursor[0], nil, nil, op) == lmdb.C.MDB_NOTFOUND then
        return false
    else
        return true
    end
end


function cursor:put(key, data, flag)
    local flag = flag or 0
    self.mdb_key = lmdb.MDB_val(self.mdb_key, key, true)
    self.mdb_data = lmdb.MDB_val(self.mdb_data, data)
    return lmdb.errcheck('mdb_cursor_put', self.mdb_cursor[0], self.mdb_key, self.mdb_data, flag)
end

function cursor:del(flag)
    local flag = flag or 0
    return lmdb.errcheck('mdb_cursor_del', self.mdb_cursor[0], flag)
end
function cursor:next()
    return self:move(lmdb.C.MDB_NEXT)
end
function cursor:prev()
    return self:move(lmdb.C.MDB_PREV)
end
function cursor:first()
    return self:move(lmdb.C.MDB_FIRST)
end
function cursor:last()
    return self:move(lmdb.C.MDB_LAST)
end


function cursor:close()
    if self.mdb_val then
        lmdb.errcheck('mdb_cursor_close', self.mdb_cursor[0])
        self.mdb_cursor = nil
    end
    self.mdb_key = nil
    self.mdb_data = nil
end




