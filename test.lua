require 'lmdb'
require 'image'
require 'trepl'

db= lmdb.env{
    Path = './testDB',
    Name = 'testDB'
}

db:open()

print(db:stat())
txn = db:txn()
x=torch.rand(3,256,256):byte()
for i=1,10 do
    txn:put(i,x)
end
txn:put(14,image.lena())
txn:commit()

print(db:stat())
local read_txn = db:txn(true)
local cursor = read_txn:cursor()
cursor:set(14)
local key,y = cursor:get()
image.display(y)

read_txn:abort()
db:close()
