# 简单的 Page
在最原始的代码中，
```Rust
pub struct Page {
    /// The data for data
    pub(crate) data: [u8; PAGE_SIZE],
}
```

只有对 `data` 一个成员变量的操作，因此如果这时只想操作 PageID 和 Page 之中所有的内容，只需要把前两个字节设计好即可，以下给出 New 的参考代码

```Rust
/// Create a new page
    /// HINT: To convert a variable x to bytes using little endian, use
    /// x.to_le_bytes()
    pub fn new(page_id: PageId) -> Self {
        let mut data = [0; PAGE_SIZE];
        // Set the page id in the first 2 bytes
        // u16 is 2 bytes
        data[0..2].copy_from_slice(&page_id.to_le_bytes());
        // Set the rest of the page to 0
        for i in 2..PAGE_SIZE {
            data[i] = 0;
        }
        Page { data }
    }
```
当然，这个页的结构太简单了。
# SlotPage 设计
根据指导书给出的经典结构
```txt
                   8 bytes                6 bytes/slot                          
        ◄──────────────────────────► ◄──────────────────────►                   
        ┌───────────────────────────┬───────────────────────┬──────┐            
      ▲ │       Page Metadata       │   Slot 1 Metadata     │      │ ▲          
      │ ├───────┬───────┐    ┌──────┼───────┬──────┐  ┌─────┤  ... │ │          
      │ │PageId │  ...  │... │  ... │ ...   │ ...  │..│...  │      │ │          
  Page│ ├───────┴───────┴────┴──────┴──────┬┴──────┴──┴─────┴──────┤ │          
Header│ │                                  │   Slot n Metadata     │ │          
      │ │  ...      ...    ...     ...     ├───────┬──────┐  ┌─────┤ │          
      │ │                                  │ ...   │ ...  │..│...  │ │          
      ▼ ├──────────────────────────────────┴───────┴──────┴──┴─────┤ │          
        │                                                          │ │          
        │  ▲                                                       │ │          
        │  │                                                       │ │          
        │  │Free Space                                             │ │          
        │  │                                                       │ │ PAGE_SIZE
        │  │                                                       │ │          
        │  │    Slot Offset                                        │ │          
        │  │    │                                                  │ │          
        │  │    ▼                                                  │ │          
        │  │    ┌─────────────────────┬────────────────────────────┤ │          
        │  │    │Value n              │Value n-1                   │ │          
        │  ▼    │                     │                            │ │          
        ├───────┴─────────────────────┴────────────────────────────┤ │          
        │                                                          │ │          
        │    ... ...          ...            ...        ...        │ │          
        │                                                          │ │          
        ├────────────────────────────────┬─────────────────────────┤ │          
        │Value 1   ...                   │ Value 0                 │ │          
        │                                │                         │ ▼          
        └────────────────────────────────┴─────────────────────────┘            
                                                          
```
可以看出在 Page 之中，我们至少需要一个 Header ，用来存储至少一个 Page Metadata 和多个 Slot Metadata。
同时还需要知道 free space 从哪开始，到哪结束。以及每个 slot 的 value 都存在哪了。
对于 value 和 free space ，由于其可变长的特性，起点、重点、大小至少需要知道两个，也可以都存储。

由于本实验简化了设计，因此需要管理的内容分别如下：
- PageId(Page Metadata)
- free space offset
- slot offset
- numbers of SlotPages
- Slot Metadata
    - slotid
    - the offset of the record
    - the length of the record

最后给出我的设计
```Rust
/// Page Header struct
pub struct Header {
    pub pagemetadata: PageMetadata,
    pub slots: Vec<Slot>,
}

/// PageMetadata struct
/// Every member's size is 2 bytes
pub struct PageMetadata {
    /// The page id
    pub page_id: PageId,
    /// The number of slots in the page
    pub num_slots: u16,
    /// The pointer to the begin of the free space
    pub offset_of_free_space: u16,
    /// The rest size of free space in the page
    pub size_of_free_space: usize,
}

/// Slot Metadata struct
pub struct Slot {
    /// The slotid
    pub slot_id: u16,
    /// The pointer to the begin of the record
    pub offsett_of_record: u16,
    /// The size of the record
    pub size_of_record: u16,
}
```
可以看到 Header 正好是 $8 + slotnums*6$ 的大小

需要注意 Page 这个变量
```Rust
pub struct Page {
    /// The data for data
    pub(crate) data: [u8; PAGE_SIZE],
}
```
似乎不应该添加其他变量，因此我们需要一些辅助函数，
在 CMSC23500 的 HW 中写的序列化作业在这里也许可以派上用场。
也就是说需要一些辅助函数来正确设置 data 的前 $8+slotnums*6$ 字节。

如果设计完这些内容，会发现 page 的 new 方法需要更新，
在new时需要把 PageMetadata 设置好,
值得庆幸的是，当确定好不再更改上述这些数据结构后
`page.rs`文件就不会再更新了。

`heap_page.rs`中
**get_header_size** 没什么可说的，$8+slotnums*6$

**get_free_space** 这个函数的名字我认为有些迷惑性，
如果我的理解正确应该是*可用空间的大小*
但是指导书写的是 *the largest block of data free in the page*
这个我不明白是否是找一个最大的“空洞”。
而且这个名字还和 heap page 的 free space 有所重复
我还是按照我的理解写的，似乎没有发生错误

**add_value** 和 **delete_value**：
本次实验一共 20 个 test，我的建议是先不管 compact 这种情况，
根据实验指导书，应该优先使用 free space。
如果顺利，当前实现可以通过前 19 个测试，只有压力测试过不去。
最后，再实现 compact 然后重构 delete/add_value

最简单的是 **delete_value**：
如果删除，只需要把 size_of_record 置零即可

**compact**:
第一按照页面中 value 的实际顺序把他们都“平移”到页面末尾
因此这里的关键是按照 offset 排序
```Rust
sort_by_key(|s| s.offset_of_record)
```
然后从最后一个开始
```Rust
for slot in valid.iter_mut().rev()
```
更新 free space 相关的两个变量

> 注意，有一个问题，移动后已经被删除的 slot 的 offset 如何处理？
> 我的处理：和 free space 的末尾对齐

**add_value**：
**本次 lab 最恶心的函数没有之一，本人重构这个函数和相关的辅助函数至少 5 次以上**
只需要遵守如下原则：
- 优先使用被删除的 slotid
- 优先使用 free space
- free space 不够的情况下尝试在 value 之间插入
- 插不进去的话需要 compact 然后再使用 free space
- 如果根本不存在被删除的 slotid，直接使用 free space，注意插入新的 slot 的话 header 的 size + 6

**HeapPageIntoIter**：
```Rust
pub struct HeapPageIntoIter {
    page: Page,
    current_slot: usize,
}
```
这里唯一的辅助变量就是一个计数器
如果完成过 KAIST CS220 的话，这个代码毫无难度

**next**:
value 是 [offset..offset + size]
## 参考
[PageHeader 的参考代码](http://blog.chinaunix.net/uid-28595538-id-4842038.html)

[SlotPages](https://siemens.blog/posts/database-page-layout/)

[blog about CMU 15-445](https://zhuanlan.zhihu.com/p/618603241)