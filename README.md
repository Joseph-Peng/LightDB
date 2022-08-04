# SimpleDB
基于Java实现的轻量版MySQL

实现了以下功能：

1. 数据的可靠性和数据恢复
2. 两段锁协议（2PL）实现可串行化调度
3. MVCC
4. 两种事务隔离级别（读提交和可重复读）
5. 死锁处理
6. 简单的表和字段管理
7. 简单的 SQL 解析
8. 基于 socket 的 server 和 client

## TM(TransactionManager)
事务管理
1. 提供了其他模块来查询、修改某个事务的状态的接口。
2. 还提供了begin、commit、abort接口来管理事务桩体

TM通过维护一个 **.tid文件** 来维护事务的状态，文件头部前8个字节
记录了管理的事务的个数。

**超级事务** ：TID为0，永远为committed状态，当一些操作想在没有申请事务的情况下进行，那么可以将操作的 TID 设置为 0。

**普通事务** ：每个事务的占用长度为1 byte，故事务在文件中的位置为
8 + tid - 1; 普通事务tid从1开始。

**TID文件合法性检查** ：每次启动数据库时需要对TID文件进行检查。
判断TID文件的长度 == 8(首部) + (事务的数量[首部8个字节记录] - 1)*事务大小。不等就说明文件错误，需要手动检查。

## DM(DataManager)

DM 直接管理数据库 DB 文件和日志文件。DM 的主要职责有：
1) 分页管理 DB 文件，并进行缓存；
2) 管理日志文件，保证在发生错误时可以根据日志进行恢复；
3) 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存。.

db文件以.db文件结尾

### 分页管理DB文件

AbstractCache: 实现了一个引用计数策略的缓存的抽象类。子类可以通过实现getForCache()和releaseForCache这两个方法来加载数据到缓存中和从缓存中释放数据

PageCacheImpl ： 页面缓存的具体实现

**PageOne：** 第一页，用于启动数据库时的检查。在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。数据库在每次启动时，就会检查第一页两处的字节是否相同
，来判断上一次是否正常关闭。

**PageX：** 普通页面，默认每一页的大小为8KB，页面的页号从1开始，前两个字节记录了当前页的空闲空间的页内偏移位置（一页的大小为8KB， 2个字节完全可以表示）。

### 日志文件管理
DM 层在每次对底层数据操作时，都会记录一条日志到磁盘上。在数据库奔溃之后，再次启动时，可以根据日志的内容，恢复数据文件，保证其一致性。
```css
[XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
XChecksum:4个字节的整数，对后续所有日志计算的校验和
Log1~LogN ： 常规的日志数据
BadTail是在数据库崩溃时，还没来得及写完的日志数据，不一定存在
```
每条日志的格式如下：
```css
[Size][Checksum][Data]
Size 是一个四字节整数，标识了 Data 段的字节数。Checksum 则是该条日志的校验和。
```
### 日志文件恢复策略
DM 为上层模块，提供了两种操作，分别是插入新数据（I）和更新现有数据（U）。至于删除数据，这个会在 VM 一节叙述。

(Ti, I, A, x)，表示事务 Ti 在 A 位置插入了一条数据 x

(Ti, U, A, oldx, newx)，表示事务 Ti 将 A 位置的数据，从 oldx 更新成 newx

在进行 I 和 U 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作。
这个日志策略，使得 DM 对于数据操作的磁盘同步，可以更加随意。日志在数据操作之前，保证到达了磁盘，那么即使该数据操作最后没有来得及同步到磁盘，数据库就发生了崩溃，后续也可以通过磁盘上的日志恢复该数据。

**规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。**

**规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。**

有了这两条规定，并发情况下日志的恢复也就很简单了：

1. 重做所有崩溃时已完成（committed 或 aborted）的事务
2. 撤销所有崩溃时未完成（active）的事务

在恢复后，数据库就会恢复到所有已完成事务结束，所有未完成事务尚未开始的状态。

### 数据读写流程
**读取数据 read(long uid)：**

从DataItem缓存中读取一个DataItem数据包并进行校验，如果DataItem缓存中没有就会调用 
	DataManager下的getForCache(long uid)从PageCache缓存中读取DataItem数据包并加入DataItem缓存
	（其实PageCache缓存和DataItem缓存都是共用的一个cache Map存的，只是key不一样，page的key是页号，
	 DataItem的key是uid，页号+偏移量），如果PgeCache也没有就去数据库文件读取。

**插入数据 insert(long tid, byte[] data)：**
先把数据打包成DataItem格式，然后在 pageIndex 中获取一个足以存储插入内容的页面的页号； 获取页面后，
	需要先写入插入日志Recover.insertLog(xid, pg, raw)，接着才可以通过 pageX 在目标数据页插入数据PageX.insert(pg, raw)，
	并返回插入位置的偏移。如果在pageIndex中没有空闲空间足够插入数据了，就需要新建一个数据页pc.newPage(PageX.initRaw())。
	最后需要将页面信息重新插入 pageIndex。
	
## VM(Version Manager)
Version Manager 是事务和数据版本的管理核心。

VM 基于两段锁协议实现了调度序列的可串行化，并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。

VM向上层抽象出entry,entry结构：[XMIN] [XMAX] [data];
XMIN 是创建该条记录（版本）的事务编号，而 XMAX 则是删除该条记录（版本）的事务编号。

XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。当想删除一个版本时，只需要设置其 XMAX，这样，这个版本对每一个 XMAX 之后的事务都是不可见的，也就等价于删除了。

### 读已提交(ReadCommitted)
只能读取已经提交事务产生的数据

假设Tid 为 Ti的事务在以下情况可以读取某一条数据

1 . 该数据由Ti创建，且还未被删除，可以读取
```text
XMIN == Ti and XMAX == null
```
或

2 . 该数据不是Ti创建，由一个已提交的事务创建，且，还未被删除或者由一个未提交的事务删除(且该事物不是Ti)
```text
XMIN is committed and  // 由一个已提交的事务创建，且
  (XMAX == null or  // 尚未删除 或
   (XMAX != Ti and XMAX is not committed) //由一个未提交的事务删除
  )
```
若条件为 true，则版本对 Ti 可见。那么获取 Ti 适合的版本，只需要从最新版本开始，依次向前检查可见性，如果为 true，就可以直接返回


### 可重复读(RepeatableRead)
事务只能读取它开始时, 就已经结束的那些事务产生的数据版本。事务需要忽略：
1. 在本事务后开始的事务的数据;
2. 本事务开始时还是 active 状态的事务的数据

对于第一条，只需要比较事务 ID，即可确定。而对于第二条，则需要在事务 Ti 开始时，记录下当前活跃的所有事务 SP(Ti)，如果记录的某个版本，XMIN 在 SP(Ti) 中，也应当对 Ti 不可见。
```text
(XMIN == Ti and                 // 由Ti创建且
 (XMAX == NULL or               // 尚未被删除
))
or                              // 或
(XMIN is commited and           // 由一个已提交的事务创建且
 XMIN < Ti and                 // 这个事务小于Ti且
 XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
 (XMAX == NULL or               // 尚未被删除或
  (XMAX != Ti and               // 由其他事务删除但是
   (XMAX is not commited or     // 这个事务尚未提交或
XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
))))

```
SP为一个Map，记录了在tid启动时状态为active的事务id

## IM(Index Manager)
基于 B+ 树的聚簇索引,目前仅支持基于索引查找数据，不支持全表扫描。

Node的结构如下：
```text
[LeafFlag][KeyNumber][SiblingUid]
[Son0][Key0][Son1][Key1]...[SonN][KeyN]

LeafFlag 标记了该节点是否是个叶子节点；
KeyNumber 为该节点中 key 的个数；
SiblingUid 是其兄弟节点存储在 DM 中的 UID。
后续是穿插的子节点（SonN）和 KeyN。
最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
```

**IM 对上层模块主要提供两种能力：插入索引和搜索节点。**

## TBM(Table Manager)
TBM主要实现两个功能, 1)利用VM维护表的结构, 2)解析并执行对应的数据库语句.

SQL解析器已经实现的 SQL 语句语法如下
```sql
<begin statement>
    begin [isolation level (read committed|repeatable read)]
        begin isolation level read committed
 
<commit statement>
    commit
 
<abort statement>
    abort
 
<create statement>
    create table <table name>
    <field name> <field type>
    <field name> <field type>
    ...
    <field name> <field type>
    [(index <field name list>)]
        create table students
        id int32,
        name string,
        age int32,
        (index id name)
 
<drop statement>
    drop table <table name>
        drop table students
 
<select statement>
    select (*|<field name list>) from <table name> [<where statement>]
        select * from student where id = 1
        select name from student where id > 1 and id < 4
        select name, age, id from student where id = 12
 
<insert statement>
    insert into <table name> values <value list>
        insert into student values 5 "Zhang Yuanjia" 22
 
<delete statement>
    delete from <table name> <where statement>
        delete from student where name = "Zhang Yuanjia"
 
<update statement>
    update <table name> set <field name>=<value> [<where statement>]
        update student set name = "ZYJ" where id = 5
 
<where statement>
    where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
        where age > 10 or age < 3
 
<field name> <table name>
    [a-zA-Z][a-zA-Z0-9_]*
 
<field type>
    int32 int64 string
 
<value>
    .*

```

TBM 基于 VM，单个字段信息和表信息都是直接保存在 Entry 中。字段的二进制表示如下：
```text
[FieldName][TypeName][IndexUid]  --> [String][String][long]
字符串的存储方式为：
[StringLength][StringData]

TypeName 为字段的类型，限定为 int32、int64 和 string 类型。
如果这个字段有索引，那个 IndexUID 指向了索引二叉树的根，否则该字段为 0。
```

本项目借鉴于[GuoZiyang](https://github.com/CN-GuoZiyang/MYDB) 和[@qw4990](https://github.com/qw4990/NYADB2) 两位大佬的开源项目