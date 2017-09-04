# LMDB for Torch
Uses ffi to wrap LMDB (http://symas.com/mdb/) functions, and Torch to serialize/deserialize objects (can be replaced by changing lmdb.[de]serialize).

## Available functions:

### lmdb.env()

```lua
open{
    Path = string                       -- Path of lmdb database
    [MapSize = number]                  -- Size of map  [default = 1099511627776]
    [NOSUBDIR = boolean]                --   [default = false]
    [NOMETASYNC = boolean]              --   [default = false]
    [RDONLY = boolean]                  --   [default = false]
    [WRITEMAP = boolean]                --   [default = false]
    [MAPASYNC = boolean]                --   [default = false]
    [NOSYNC = boolean]                  --   [default = false]
    [NOTLS = boolean]                   --   [default = false]
    [NOLOCK = boolean]                  --   [default = false]
    [Mode = number]                     --   [default = 664]
    [MaxDBs = number]                   --   [default = 1]
    [MaxReaders = number]               --   [default = 3]
    [Name = string]                     --   [default = Data]
}

txn(rdonly, parent_txn)
reader_check()
stat()
```
### lmdb.txn(env_obj, rdonly, parent_txn)
```lua
dbi_open(name, flags)
commit()
abort()
reset()
renew()
put(key, data, flag)
get(key)
cursor() 
```
### lmdb.cursor()
```lua
get(op)
put(key, data, flag)
del(flag)
close()
```

## Usage Example
```lua
require 'lmdb'

local db= lmdb.env{
    Path = './testDB',
    Name = 'testDB'
}

db:open()
print(db:stat()) -- Current status
local txn = db:txn() --Write transaction
local cursor = txn:cursor()
local x=torch.rand(10,3,256,256):byte()

-------Write-------
for i=1,10 do
    txn:put(i,x[i])
end
txn:commit()

local reader = db:txn(true) --Read-only transaction
local y = torch.Tensor(10,3,256,256)

-------Read-------
for i=1,10 do
    y[i] = reader:get(i)
end

reader:abort()

db:close()
```




