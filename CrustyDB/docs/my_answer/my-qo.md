# operators

这个 lab 对看代码的要求比前两个作业更大
需要看一下
```Rust
use common::bytecode_expr::ByteCodeExpr;
use common::{CrustyError, Field, TableSchema, Tuple};
```
这里面的代码

## NestedLoopJoin
数据结构设计
```Rust
pub struct NestedLoopJoin {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    op: BooleanOp,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    // maintain operator state here
    current_left: Option<Tuple>,
}
```
这个设计主要是因为
```Rust
fn configure(&mut self, will_rewind: bool) {
    self.left_child.configure(will_rewind);
    self.right_child.configure(true); // right child will always be rewound by NLJ
}
```
也就是说，在遍历 right_child 时，当前的 left_child 元组需要保持不变。

唯一复杂的是 next 函数，注意这里不能写出双重 for 循环，而是要把 left_child 取出的元组存下来

## HashJoin
```rust
pub struct HashEqJoin {
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    schema: TableSchema,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    // States (Need to reset on close)
    hash_table: HashMap<Field, Vec<Tuple>>,
}
```

这里我没搞懂 manager 到底有什么用，不过似乎不影响写代码，
所以以下我贴了 ChatGPT 的回复：
> `managers: &'static Managers`: 这是一个新的字段，它持有一个对 `'static` 生命周期 `Managers` 结构体的引用。这表明 HashEqJoin 依赖于一个全局或静态的 Managers 实例，可能用于管理一些共享的资源或配置。

仍然要注意
```rust
fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(false); // left child will never be rewound by HJ
        self.right_child.configure(will_rewind);
    }
```

`open` 构建哈希表，也就是说在 open 阶段
会消耗掉左子操作符的所有输出，并将其存储在哈希表中

`next` 处理连接的情况，注意HasnJoin 需要处理一个右侧元组与多个左侧元组匹配的情况,
不过如果简化成一对一匹配，这种简化的设计仍然可以通过本地测试。

## Aggregate
非常复杂的一个部分，这里我不确定我的答案最优或者是否有冗余设计
```rust
enum AggregateState {
    Count(i64),
    Sum(Option<Field>),
    Min(Option<Field>),
    Max(Option<Field>),
    Avg { sum: Option<Field>, count: i64 },
}
pub struct Aggregate {
    managers: &'static Managers,
    schema: TableSchema,
    groupby_expr: Vec<ByteCodeExpr>,
    agg_expr: Vec<ByteCodeExpr>,
    ops: Vec<AggOp>,
    child: Box<dyn OpIterator>,
    will_rewind: bool,
    groups: HashMap<Vec<Field>, Vec<AggregateState>>,
    result_buffer: Option<Vec<Tuple>>,
    buffer_iterator_idx: usize,
    open: bool,
}
```
**groups** 是核心，Key 是groupby_expr 列表计算得到的分组键
**result_buffer** 也许是一个必须的设计，不然无法完成 rewind 

`merge_tuple_into_group`: 
比较简单，先遍历 self.groupby_expr，
然后遍历 self.agg_expr
最后写一个超大的 match 来设置和更新聚合状态 AggregateState

当然还需要最后把 AggregateState 转换为一个结果 Tuple，
这里写不写辅助函数都可以，只有 Avg处理比较麻烦

`rewind` 这里卡了很久，只剩一个测试一直过不去，在一次 debug 过程中偶然通过了测试：
如果 self.will_rewind 为 true，则直接返回 Ok(None)，
也就是说重新执行整个聚合过程。
## 参考
[NestedLoopJoin](https://zhuanlan.zhihu.com/p/411723794)
[NestedLoopJoin](https://blog.csdn.net/qq_42600094/article/details/130404212)
[hashjoin](https://zhuanlan.zhihu.com/p/663637344)
[hashjoin](https://zhuanlan.zhihu.com/p/94065716)