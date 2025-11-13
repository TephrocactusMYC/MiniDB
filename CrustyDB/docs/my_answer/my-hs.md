# heapstorage
## HeapFile
首先我们看指导书中关于这个结构体的示意：
```
            ┌───►        ┌─────────────┐ ◄──┐                          
            │            │             │    │                          
            │            │             │    │                          
            │            │             │    │  
            │            │             │    │                          
            │            │             │    │  4096-byte-sized HeapPage                      
            │            │             │    │                          
            │            │             │    │                          
            │            │             │    │                          
            │            ├─────────────┤ ◄──┘                          
            │            │             │                               
            │            │             │                               
            │            │             │                               
  HeapFile  │            │             │                               
(A sequence │            │             │                               
    of      │            │             │                               
 HeapPages) │            │             │                               
            │            │             │                               
            │            ├─────────────┤                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            │            │             │                               
            └──►         └─────────────┘                                                             
```
这个实现相比其他课程讲的要简化一些，在 CMU 15-445 中
HeapFile应该是以指针的形式存储每个页，
而在这个设计中，每个页在地址上连续存储在一起

最后给出HeapFile的设计
```Rust
pub(crate) struct HeapFile {
    // TODO milestone hs
    // Add any fields you need to maintain state for a HeapFile
    file: Arc<RwLock<File>>, // file

    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}
```
如果本着简化实验的目的，我们只需要新增这一个成员变量

这里除了需要熟悉 IO 操作之外，唯一需要注意的点是如何获取每个页，
可以考虑根据下面的公式
$$ PageId = offset / PAGE\_SIZE $$

## Heap File Iterator
直接根据提示
```Rust
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    tid: TransactionId,
    current_page_id: PageId,
    current_page_iter: Option<HeapPageIntoIter>,
}
```
这个迭代器应该用一个循环实现，从 current_page_id 开始

## Storage Manager
需要熟悉和文件夹操作相关的一些 API

其他大部分函数都可以通过调用 Heapfile 内的函数来实现

`insert_value` 函数确实最难，不过幸好框架排除了最难的情况
```Rust
if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
```
整体实现逻辑大概如下：
1. 先遍历，找有没有能插入的页，如果有， write_page_to_file
2. 如果没有，申请一个新页然后插入

如果你的 lab1 和 lab2 间隔时间很长，也许需要回头看一下你曾实现过的 API

有意思的是， `shutdown`函数应该怎么做？答案是什么都不做。
## 简单总结
总之，这个作业并不难，不过你需要熟悉
```Rust
use std::fs;
use std::path::{Path, PathBuf};
```
对于 IO 的操作——对文件夹和单个文件的。并且掌握
```Rust
use std::sync::{Arc, RwLock};
```
的基本用法，虽然本次 lab 并没有使用到什么关于并发的特性

至于 buffer_pool ，由于缺乏文档，并且和 CMU 15-445 重复，我没有实现