#!/usr/bin/env python
#coding:utf-8

'''
Created on 2016年11月15日

@author: lichen
'''

import redis
import time

from redis import WatchError
from redis.sentinel import Sentinel

def string_test(r):
    """
    string类型操作方法示例
    """
    r.set("foo","lichentest1111111111111") #在Redis中设置值;默认，不存在则创建，存在则修改。
    print r.get("foo")
    r.setnx("foo","bar") #只有key不存在时，执行设置操作;相当于新建、添加操作
    r.setex("foo","bar",5) #设定过期时间为5s；5s以后key:"foo"的值变为None
    r.psetex("foo",5000,"bar") #设定设定过期时间为5000ms；5000ms以后key:"foo"的值变为None
    r.mset(**{"foo1":"bar1","foo2":"bar2"}) #批量设置
    print r.mget(*["foo1","foo2","foo4","foo5"]) #批量获取
    print(r.getset("foo","new_bar")) #设置新值并获取原来值
    r.setrange("foo",1,"bbb") #指定字符串索引位置开始替换内容
    print (r.getrange("foo",0,2)) #取value中从第1字节开始到第3字节(切片操作)。（一个汉字3个字节 1个字母一个字节 每个字节8bit）
    print r.strlen("foo") #获取指定key的值的字节长度
    r.incr("bing",amount=1) #自增指定key的值(amount必须为整数)；当key不存在时，则创建key=amount；否则自增
    r.incrbyfloat("bing",amount=1.1) #自增指定key的值(amount必须为浮点型)；当key不存在时，则创建key=amount；否则自增
    r.decr("bing",amount=1) #自减指定key的值(amount必须为整数)；当key不存在时，则创建key=amount；否则自减
    r.append("foo","addcontext") #在指定ket的值后追加新内容

def hash_test(r):
    """
    hash类型操作方法示例
    """
    r.hset("foo_hash","k1","v1") #添加单个hash
    r.setnx("foo_hash1","k1","v1") #只有key不存在时，执行设置操作;相当于新建、添加操作
    print r.hget("foo_hash","k1") #取单一值；key+filed
    r.hmset("foo_hash2",{"k1":"v1","k2":"v2"}) #批量添加hash
    print r.hmget("foo_hash2",*["k1","k2"]) #批量取出多个值；key+多个filed
    print r.hgetall("foo_hash2") #取出指定key对应的hashmap
    print r.hkeys("foo_hash2") #取出指定key对应的hashmap中的所有key
    print r.hvals("foo_hash2") #取出指定key对应的hashmap中的所有value
    print r.hexists("foo_hash2","k3") #查询指定key对应的hashmap中key是否存在;只能查单个key
    r.hdel("foo_hash2",*["k2","k1"]) #删除指定key对应的hashmap中的键值对;删除多个时使用函数的可变参数属性即可
    r.hincrby("foo_hash1","k1",amount=-1) #自增或自减指定key对应的hashmap中特定key的value(负数为自减；必须为整数)
    r.incrbyfloat("foo_hash1","k1",amount=-1.0) #自增或自减指定key对应的hashmap中特定key的value(负数为自减；必须为浮点型)
    #分片读取；对大量数据效果明显；直到返回值cursor的值为0时，表示数据已经通过分片获取完毕
    cursor1,data1 = r.hscan('xx', cursor=0, match=None, count=None) #返回的data为指定key对应的hashmap(dict)
    cursor2,data1 = r.hscan('xx', cursor=cursor1, match=None, count=None)
    #利用yield封装hscan创建生成器，实现分批去redis中获取数据
    print(r.hscan_iter("foo_hash1")) #生成器内存地址
    for item in r.hscan_iter('foo_hash1'):
        print item #返回的item为包涵hashmap每一组的键值对内容的tuple

def list_iter(rlist,rlist_name):
    """
    列表元素生成器
    
    :param rlist
            redis对象实例
    :param rlist_name
            redis队列名
            
    :return yield(返回生成器)
    """
    list_count=rlist.llen(rlist_name)
    for index in xrange(list_count):
        yield rlist.lindex(rlist_name,index)

def list_test(r):
    """
    list类型操作方法示例
    """
    """
    r.lpush("foo_list",*[11,22,33]) #从列表起始位置(最左侧)添加数据；若列表不存在则新建
    r.rpush("foo_list",*[11,22,33]) #从列表结尾位置(最右侧)添加数据；若列表不存在则新建
    r.lpushx("foo_list",*[11,22,33]) #从列表起始位置(最左侧)添加数据；只有列表存在时执行
    r.rpushx("foo_list",*[11,22,33]) #从列表结尾位置(最右侧)添加数据；只有列表存在时执行
    r.linsert("foo_list","before","22","444") #在列表指定中从左边第一个的元素“22”的前边插入新元素“444”
    r.linsert("foo_list","after","11","555") #在列表指定中从左边第一个的元素“11”的后边插入新元素“555”
    r.lset("foo_list",5,"666") #将列表中的索引号为5的元素修改成"666"
    r.lrem("foo_list",2,"22") #删除列表中从左边(从前向后)第一个出现的“22”和第二个出现的“22”
    r.lrem("foo_list",-2,"11") #删除列表中从右边(从后向前)第一个出现的“11”和其前边的一个元素
    r.lrem("foo_list",1,"33") #删除列表中从左边第一个出现的“33”
    r.lrem("foo_list",0,"33") #删除列表中所有的"33"
    r.ltrim("foo_list",9,9) #删除除指定范围的索引号之外的元素；起始和结束大于列表最大长度时清空列表
    print r.lpop("foo_list") #弹出左边第一个元素(队首)，并返回该元素
    print r.rpop("foo_list") #弹出右边第一个元素(队尾)，并返回该元素
    print r.lindex("foo_list",1) #取出队列中指定索引号的元素
    r.rpoplpush("foo_list","foo_list1") #从"foo_list"中取对尾元素并添加到"foo_list1"的队首位置
    r.brpoplpush("foo_list","foo_list1",timeout=0) #阻塞版rpoplpush；当timeout=0时表示永久阻塞(永久等待第一个队列中出现值)
    """
    """
    while 1:
        print r.brpop("foo_list",timeout=0)[1] #rpop的阻塞版;返回由list名和元素组成的tuple;当timeout=0时表示永久阻塞(永久等待队列中出现值)
    """
    #遍历队列中所有的元素;调用自定义的列表元素生成器    
    for item in list_iter(r,"foo_list"):
        print item,
    print "\n",r.lrange("foo_list",0,-1) #根据索引号切片取值;返回一个list
    print r.lrange("foo_list1",0,-1)
    print r.llen("foo_list") #返回列表长度

def set_test(r):
    """
    set类型操作方法示例
    
    :param r
            redis对象实例
    
    :return Null
    """
    #r.sadd("foo_set",*[11,22,33]) #向指定集合中添加元素
    #r.sadd("foo_set1",*[33,44,55])
    print r.sdiff("foo_set","foo_set1") #返回两个集合的差集；返回一个set对象实例
    r.sdiffstore("foo_dset","foo_set","foo_set1")#返回两个集合的差集，并将结果保存到新集合中
    print r.sscan("foo_dset")[1]
    print r.sinter("foo_set","foo_set1") #返回两个集合的交集；返回一个set对象实例
    r.sinterstore("foo_dset","foo_set","foo_set1") #返回两个集合的交集，并将结果保存到新集合中
    print r.sunion("foo_set","foo_set1") #返回两个集合的并集；返回一个set对象实例
    r.sunionstore("foo_dset","foo_set","foo_set1") #返回两个集合的并集，并将结果保存到新集合中
    print r.sismember("foo_set",11) #检查元素是否为集合中的成员；返回布尔值
    print r.smembers("foo_set") #获取集合中所有的成员;返回一个set对象实例
    r.smove("foo_set","foo_set1",11) #将某一成员从一个集合移动到另外一个集合
    r.spop("foo_set") #从集合中随机删除一个成员；因为集合是无序的，所以是随机删除
    r.srem("foo_set",22) #删除集合中的指定值
    print r.srandmember("foo_set1",2) #随机获取一个集合中的多个成员；返回一个包涵多个成员的list
    print r.scard("foo_set") #获取集合中成员个数
    print r.sscan("foo_set")[1] #获取集合中所有成员－元祖形式;返回一个list，list[1]为包涵所有成员的list
    print r.sscan("foo_set1")[1]
    #使用迭代器分片获取集合成员
    for item in r.sscan_iter("foo_set"):
        print item
 
def sorted_set_test(r):
    """
    有序set类型操作方法示例
    
    :param r
            redis对象实例
    
    :return Null
    """
    r.zadd("foo_zset",**{"n1":11,"n2":22}) #向有序集合中添加元素和对应分数 
    print r.zcard("foo_zset") #返回有序集合长度
    print r.zscore("foo_zset","n1") #获取指定成员的分数
    print r.zrange("foo_zset",0,-1) #返回有序集合中的所有成员;从小到大排序;只取元素不取分数；返回list
    print r.zrange("foo_zset",0,-1,withscores=True)#返回有序集合中的所有成员和其对应分数;返回保存所有由元素和分数组成的tuple的list
    print r.zrevrange("foo_zset",0,-1) #返回有序集合中的所有成员;从大到小排序;只取元素不取分数；返回list
    print r.zrangebyscore("foo_zset",15,25) #按照规定分数范围返回有序集合中成员；从小到大排序；返回list
    print r.zrevrangebyscore("foo_zset",15,25) #按照规定分数范围返回有序集合中成员；从大到小排序；返回list
    print r.zscan("foo_zset")[1] #获取有序集合中所有元素；按照分数从小到大排序；返回list
    #使用生成器返回有序集合所有元素;返回由成员和其对应分数组成的tuple
    for item in r.zscan_iter("foo_zset"):
        print "member=",item[0],"score=",item[1]
    print r.zcount("foo_zset",20,30) #返回在规定分数范围内的成员数量
    r.zincrby("foo_zset","n2",amount=2) #执行该方法一次，对应成员的分数增加2
    print r.zrank("foo_zset","n2") #获取该成员在有序集合中的排名；按分数从小到大排序;索引号从0开始计算
    print r.zrevrank("foo_zset","n2") #获取该成员在有序集合中的排名；按分数从大到小排序
    r.zrem("foo_zset",*["n1",]) #删除有序集合中特定成员
    r.zremrangebyrank("foo_zset",0,4) #按排行删除成员；按分数从小到大排行
    r.zremrangebyscore("foo_zset",23,59) #按规定分数范围删除成员
    
    
def watch_test(pipe):
    """
    自定义管道监控回调方法
    """
    pipe.get("foo")
    pipe.multi() #用MULTI命令把pipeline设置成缓冲模式
    pipe.set("bing","222222222222")
        
def pipe_test(r):
    """
    管道操作示例
    """
    pipe=r.pipeline()
    while 1:
        try:
            #监视的key
            pipe.watch("bing")
            #pipe.set("bing","222222222222")
            #pipe.get("foo")
            #pipe.execute()
            #所有缓冲到 pipeline 的命令返回 pipeline 对象本身。因此调用可以这样使用:
            pipe.set("bing","222222222222").get("foo").execute()
            #执行成功退出循环
            break 
        except WatchError:
            continue
        finally:
            pipe.reset()
    #上述的操作还可以使用with语句和transaction方法实现
    #使用with方式会自动调用reset方法
    while 1:
        with r.pipeline() as pipe:
            try:
                pipe.watch("bing") #监视的key
                #pipe.get("foo")
                #pipe.multi() #用MULTI命令把pipeline设置成缓冲模式
                #pipe.set("bing","222222222222")
                #pipe.execute()
                #执行成功退出循环
                break 
            except WatchError:
                continue
            #transaction方法:参数是一个可执行对象和要 WATCH 任意个数的键，其中可执行对象接受一个pipeline对象做为参数
            #r.transaction(watch_test,"bing")

def pub_test(message):
    """
    自定义订阅回调方法
    """    
    print "这里是自定义订阅回调方法。订阅消息内容:",'"',message["data"],'"'

def pubsub_test(r):
    """
    发布/订阅操作示例
    """
    p=r.pubsub() #创建订阅监听对象
    #
    #p.subscribe("redisChat") #订阅指定的一个或多个频道
    p.subscribe(**{"redisChat":pub_test}) #使用自定义订阅回调方法来处理订阅信息内容
    #print r.publish("redisChat","client_test11111") #发布消息
    #持续监听频道
    while 1:
        m=p.get_message() #注意同一条message只能被消费一次
        if m:
            #过滤空数据情况
            if type(m["data"]).__name__ != "long":
                print m["data"]
        time.sleep(2) #防止多进程或线程发生死锁
    #当pubsub对象完成任务时，关闭它
    p.close()
    
def sentinel_test():
    """
    监控redis节点操作示例
    """
    sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    #根据服务标签名查找主节点。若查找到则返回主节点ip和端口号的tuple；若没找到则返回错误MasterNotFoundError
    sentinel.discover_master('mymaster')
    #根据服务标签名查找从节点。若查找到则返回活动的从节点ip和端口号的list；若没找到则返回空list
    sentinel.discover_slaves('mymaster')
    #还可以使用监控实例来创建redis客户端来监控节点
    master = sentinel.master_for('mymaster', socket_timeout=0.1)
    slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    master.set('foo', 'bar')
    slave.get('foo','bar')

def main():
    #StrictRedis类实现了大部分redis官方命令;Redis类是用来兼容旧版本的redis.py;尽量使用StrictRedis类
    r=redis.StrictRedis(host="localhost",port=6379,db=0)
    """
    #可以直接建立一个连接池，然后作为参数Redis，这样就可以实现多个Redis实例共享一个连接池
    pool = redis.ConnectionPool(host='192.168.19.130', port=6379)
    r = redis.Redis(connection_pool=pool) 
    """
    #string_test(r)
    #hash_test(r)
    #pipe_test(r)
    #pubsub_test(r)
    #sentinel_test()
    #list_test(r)
    #set_test(r)
    #sorted_set_test(r)

if __name__ == '__main__':    
    main()














