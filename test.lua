require 'lmdb'
require 'image'
db= lmdb.DB()
db:open{
    Path = './testDB',
    Name = 'testDB'
}

x=torch.rand(3,256,256):byte()
key = 1--torch.Tensor(1):fill(1)
for i=1,10 do
db:put(i,x)
end

db:put(4,image.lena():byte())
db:commit()
y = db:get(4)
image.display(y)
--print(y)

