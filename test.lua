require 'lmdb'
require 'image'
db= lmdb.env()
db:open{
    Path = './testDB',
    Name = 'testDB'
}


print(db:stat())
txn = db:txn()
cursor = txn:cursor()
x=torch.rand(3,256,256):byte()
for i=1,10 do
    txn:put(i,x)
end
--
txn:put(14,image.lena():byte())
txn:commit()

--z = txn:get(1)
--y = db:get(4)
--image.display(y)
----print(y)

