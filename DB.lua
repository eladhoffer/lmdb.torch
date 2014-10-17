local DB = torch.class('lmdb.DB')

local C = lmdb.C
local ffi = require 'ffi'
require 'xlua'
require 'dok'
local errcheck = lmdb.errcheck
local from_MDB_val = lmdb.from_MDB_val
local MDB_val = lmdb.MDB_val
function DB:__init()
    self.mdb_env = ffi.new('MDB_env *[1]')
    errcheck('mdb_env_create',self.mdb_env)
    local function destroy_env(x)
        errcheck('mdb_env_close',x)

    end
    self.mdb_env = self.mdb_env[0]
    ffi.gc(self.mdb_env, destroy )

    self.mdb_dbi = ffi.new('MDB_dbi[1]')

    self.mdb_txn = ffi.new('MDB_txn *[1]')

end

function DB:open(...)
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
    {arg='NOTLS', type='boolean', help='', default = true},
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
    errcheck('mdb_env_set_mapsize',self.mdb_env, args.MapSize) 
    errcheck('mdb_env_set_maxdbs',self.mdb_env, args.MaxDBs) 
    errcheck('mdb_env_set_maxreaders',self.mdb_env, args.MaxReaders) 
    local path = paths.concat(args.Path)
    PATH = path
    os.execute('mkdir -p "' .. paths.dirname(path) .. '"')
    errcheck('mdb_env_open',self.mdb_env,path, flags, tonumber(args.Mode,8))
    errcheck('mdb_txn_begin',self.mdb_env, nil, 0, self.mdb_txn)
    errcheck('mdb_dbi_open', self.mdb_txn[0], nil, 0, self.mdb_dbi)
    --errcheck('mdb_cursor_open', self.mdb_txn[0], self.mdb_dbi[0], self.mdb_cursor)
end

function DB:get_next()
    local mdb_key = ffi.new('MDB_val[1]')
    local mdb_data = ffi.new('MDB_val[1]')

    errcheck('mdb_cursor_get', self.mdb_cursor[0], mdb_key, mdb_data, C.MDB_NEXT)

    return from_MDB_val(mdb_data)
end

function DB:put(key, data, flag)
    local flag = flag or 0
    local mdb_key = MDB_val(key)
    local mdb_data = MDB_val(data)
    errcheck('mdb_put', self.mdb_txn[0], self.mdb_dbi[0], mdb_key,mdb_data, flag)

end
function DB:commit()
    errcheck('mdb_txn_commit', self.mdb_txn[0])
end

function DB:get(key)
    local read_txn = ffi.new('MDB_txn *[1]')
    errcheck('mdb_txn_begin',self.mdb_env, nil, C.MDB_RDONLY, read_txn)
    local mdb_key = MDB_val(key)
    local mdb_data = ffi.new('MDB_val[1]')
    errcheck('mdb_get', read_txn[0], self.mdb_dbi[0], mdb_key,mdb_data)
    return from_MDB_val(mdb_data)
end

function DB:get_first()
    local mdb_key = ffi.new('MDB_val[1]')
    local mdb_data = ffi.new('MDB_val[1]')

    errcheck('mdb_cursor_get',self.mdb_cursor[0], mdb_key, mdb_value, C.MDB_FIRST)
    return from_MDB_val(mdb_data)
end
function DB:open_cursor()
    self.mdb_cursor = ffi.new('MDB_cursor *[1]')
    errcheck('mdb_cursor_open',self.mdb_txn[0], self.mdb_dbi[0], self.mdb_cursor)
end


function DB:close()
    C['mdb_cursor_close'](self.mdb_cursor)
    C['mdb_dbi_close'](self.mdb_env, self.mdb_dbi)
    C['mdb_txn_abort'](self.mdb_txn)
    C['mdb_env_close'](self.mdb_env)
end

