#pragma once

#include <queue>

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"
#include <iostream>
#include <string>

namespace wing {

namespace lsm {

template <typename T>
class IteratorHeap final : public Iterator {
 public:
    IteratorHeap() = default;

    void Push(T* it) { 

     it_.push(it);

    }

    void Pop(){

        it_.pop();

    }

    void Build() { 

    }

    bool Valid() override { 

        return !(it_.empty()) && it_.top()->Valid();

    }

    Slice key() const override { 

        if (!(it_.empty()) && it_.top()->Valid()) {
            return it_.top()->key();
        } 
        else {
            return Slice();
        }

    }

    Slice value() const override { 

        if (!(it_.empty()) && it_.top()->Valid()) {

            return it_.top()->value();

        } 
        else {
            return Slice();
        }

    }

    void Next() override { 

        if (!(it_.empty()) && it_.top()->Valid()) {
            T* top_it = it_.top();
            top_it->Next(); 
            if (!top_it->Valid()) {
                it_.pop(); 
            } 
            else {
                it_.pop();
                it_.push(top_it);
            }
        }

    }

    T* Top() {

        return it_.top();

    }

    void Clear() {

        std::priority_queue<T*, std::vector<T*>, Comparator> empty_it;
        std::swap(it_, empty_it);

    }

    private:

        struct Comparator{

            bool operator()(T* a, T* b) const {

                return ParsedKey(a->key()) > ParsedKey(b->key());

            }
        };

        std::priority_queue<T*, std::vector<T*>, Comparator> it_;

};

}  // namespace lsm

}  // namespace wing
