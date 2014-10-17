require 'lmdb'
require 'image'
db= lmdb.DB()
db:open{
    Path = './',
    Name = 'testDB'
}

x=torch.rand(3,256,256)
key = 1--torch.Tensor(1):fill(1)
for i=1,10 do
db:put(i,x)
end

db:put(4,image.lena())
db:commit()
--y = db:get(key)
--print(y)

