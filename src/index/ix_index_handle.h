/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "ix_defs.h"
#include "transaction/transaction.h"

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;

// 比较两个字符串，根据数据类型返回比较结果
inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

// 比较两个复合键，根据列类型和长度返回比较结果
inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    Page *page;                     // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址

   public:
    IxNodeHandle() = default;

    // 构造函数，初始化节点句柄
    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    // 获取节点中键的数量
    int get_size() { return page_hdr->num_key; }

    // 设置节点中键的数量
    void set_size(int size) { page_hdr->num_key = size; }

    // 获取节点的最大键数量
    int get_max_size() { return file_hdr->btree_order_ + 1; }

    // 获取节点的最小键数量
    int get_min_size() { return get_max_size() / 2; }

    // 获取第i个键的值
    int key_at(int i) { return *(int *)get_key(i); }

    // 获取第i个孩子节点的page_no
    page_id_t value_at(int i) { return get_rid(i)->page_no; }

    // 获取节点的page_no
    page_id_t get_page_no() { return page->get_page_id().page_no; }

    // 获取节点的PageId
    PageId get_page_id() { return page->get_page_id(); }

    // 获取下一个叶子节点的page_no
    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    // 获取前一个叶子节点的page_no
    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    // 获取父节点的page_no
    page_id_t get_parent_page_no() { return page_hdr->parent; }

    // 判断节点是否为叶子节点
    bool is_leaf_page() { return page_hdr->is_leaf; }

    // 判断节点是否为根节点
    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    // 设置下一个叶子节点的page_no
    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    // 设置前一个叶子节点的page_no
    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    // 设置父节点的page_no
    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    // 获取第key_idx个键的指针
    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    // 获取第rid_idx个Rid的指针
    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    // 设置第key_idx个键的值
    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    // 设置第rid_idx个Rid的值
    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    // 在节点中查找第一个大于等于target的键的位置
    int lower_bound(const char *target) const;

    // 在节点中查找第一个大于target的键的位置
    int upper_bound(const char *target) const;

    // 在节点中的指定位置插入多个键值对
    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    // 在内部节点中查找键对应的子节点的page_no
    page_id_t internal_lookup(const char *key);

    // 在叶子节点中查找键对应的Rid
    bool leaf_lookup(const char *key, Rid **value);

    // 在节点中插入键值对
    int insert(const char *key, const Rid &value);

    // 在节点中的指定位置插入单个键值对
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    // 删除节点中的指定位置的键值对
    void erase_pair(int pos);

    // 删除节点中的指定键
    int remove(const char *key);

    // 删除根节点中的最后一个键，并返回最后一个孩子节点
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    // 由parent调用，寻找child，返回child在parent中的rid_idx
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }
};

/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;                                    // 存储B+树的文件
    IxFileHdr* file_hdr_;                       // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::mutex root_latch_;

   public:
    // 构造函数，初始化B+树句柄
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    // 根据键获取对应的Rid
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    // 查找键对应的叶子节点
    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                 bool find_first = false);

    // 插入键值对到B+树中
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    // 分裂节点
    IxNodeHandle *split(IxNodeHandle *node);

    // 插入新节点到父节点中
    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // 删除键值对
    bool delete_entry(const char *key, Transaction *transaction);

    // 合并或重新分配节点
    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);

    // 调整根节点
    bool adjust_root(IxNodeHandle *old_root_node);

    // 重新分配节点
    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    // 合并节点
    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched);

    // 查找第一个大于等于键的位置
    Iid lower_bound(const char *key);

    // 查找第一个大于键的位置
    Iid upper_bound(const char *key);

    // 获取叶子节点的结束位置
    Iid leaf_end() const;

    // 获取叶子节点的开始位置
    Iid leaf_begin() const;

   private:
    // 更新根节点的page_no
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    // 判断B+树是否为空
    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // 获取指定page_no的节点
    IxNodeHandle *fetch_node(int page_no) const;

    // 创建新节点
    IxNodeHandle *create_node();

    // 维护节点的父节点信息
    void maintain_parent(IxNodeHandle *node);

    // 删除叶子节点
    void erase_leaf(IxNodeHandle *leaf);

    // 释放节点句柄
    void release_node_handle(IxNodeHandle &node);

    // 维护节点的孩子节点信息
    void maintain_child(IxNodeHandle *node, int child_idx);

    // 根据Iid获取Rid
    Rid get_rid(const Iid &iid) const;
};