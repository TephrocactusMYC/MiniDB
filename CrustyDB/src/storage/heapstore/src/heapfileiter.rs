use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    tid: TransactionId,
    current_page_id: PageId,
    current_page_iter: Option<HeapPageIntoIter>,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        // panic!("TODO milestone hs");
        let mut iter = HeapFileIterator {
            hf,
            tid,
            current_page_id: 0,
            current_page_iter: None,
        };
        iter.load_next_page_iter(); // 尝试初始化第一页的迭代器
        iter
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        // panic!("TODO milestone hs");
        let mut iter = HeapFileIterator {
            hf,
            tid,
            current_page_id: value_id.page_id.expect("REASON"),
            current_page_iter: None,
        };
        iter.load_next_page_iter(); // 定位到指定页
        iter
    }

    fn load_next_page_iter(&mut self) {
        while self.current_page_id < self.hf.num_pages() {
            match self.hf.read_page_from_file(self.current_page_id) {
                Ok(page) => {
                    self.current_page_iter = Some(page.into_iter());
                    return;
                }
                Err(_) => {
                    // 跳过无法读取的页，尝试下一页
                    self.current_page_id += 1;
                }
            }
        }
        self.current_page_iter = None;
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        // panic!("TODO milestone hs");
        loop {
            if let Some(iter) = &mut self.current_page_iter {
                if let Some((data, slot_id)) = iter.next() {
                    let value_id = ValueId {
                        container_id: self.hf.container_id,
                        segment_id: None,
                        page_id: Some(self.current_page_id),
                        slot_id: Some(slot_id),
                    };
                    return Some((data, value_id));
                } else {
                    self.current_page_id += 1;
                    self.load_next_page_iter();
                }
            } else {
                return None;
            }
        }
    }
}
