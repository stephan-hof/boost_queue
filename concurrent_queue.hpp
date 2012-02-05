/*
 * Copyright Stephan Hofmockel 2012.
 * Distributed under the Boost Software License, Version 1.0.
 * See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt
*/

#ifndef CONNCURENT_QUEUE_HPP
#define CONNCURENT_QUEUE_HPP 

#include <deque>
#include <exception>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread_time.hpp>

class QueueEmpty : public std::exception {};
class QueueFull : public std::exception {};
class NoMoreTasks : public std::exception {};

template<typename T>
class ConcurrentQueue {
    protected:
        boost::mutex mutex;
        std::deque<T> queue;
        boost::condition_variable empty_cond;
        boost::condition_variable full_cond;
        boost::condition_variable all_tasks_done_cond;
        size_t maxsize;
        boost::uint64_t unfinished_tasks;

    public:
        ConcurrentQueue(long maxsize=0);
        size_t size();
        size_t get_maxsize(){return this->maxsize;};
        /* block=true and timeout=0 => same as block= false */
        /* timeout=0 => timeout is *not* considered */
        /* Timeout is in milliseconds */
        void put(T&, bool block=true, boost::uint64_t timeout=0);
        void pop(T&, bool block=true, boost::uint64_t timeout=0);
        void task_done();
        void join();
};

template<typename T>
ConcurrentQueue<T>::ConcurrentQueue(long maxsize)
{
    if (maxsize < 0) {
        this->maxsize = 0;
    }
    else {
        this->maxsize = maxsize;
    }
    this->unfinished_tasks = 0;
}

template<typename T>
size_t
ConcurrentQueue<T>::size()
{
    boost::lock_guard<boost::mutex> raii_lock(this->mutex);
    return this->queue.size();
}

template<typename T>
void
ConcurrentQueue<T>::put(T &item, bool block, boost::uint64_t timeout)
{
    boost::mutex::scoped_lock lock(this->mutex);

    if ((this->queue.size() < this->maxsize) or (this->maxsize == 0)) {
        /* Fall through the end of method */
    }
    else if (not block) {
        throw QueueFull();
    }
    else {
        if (timeout > 0) {
            boost::system_time abs_timeout = boost::get_system_time();
            abs_timeout += boost::posix_time::milliseconds(timeout);
            while (this->queue.size() ==  this->maxsize) {
                if (not this->full_cond.timed_wait(lock, abs_timeout)) {
                    throw QueueFull();
                }
            }
        }
        else {
            while (this->queue.size() == this->maxsize) {
                this->full_cond.wait(lock);
            }
        }
    }

    this->queue.push_back(item);
    this->unfinished_tasks += 1;
    lock.unlock();
    this->empty_cond.notify_one();
}
 
template<typename T>
void
ConcurrentQueue<T>::pop(T & item, bool block, boost::uint64_t timeout)
{
    boost::mutex::scoped_lock lock(this->mutex);
    if (not this->queue.empty()) {
        /* Fall through the end of method */
    }
    else if (not block){
        throw QueueEmpty();
    }
    else {
        if (timeout > 0) {
            boost::system_time abs_timeout = boost::get_system_time();
            abs_timeout += boost::posix_time::milliseconds(timeout);
            while (this->queue.empty()) {
                if (not this->empty_cond.timed_wait(lock, abs_timeout)) {
                    throw QueueEmpty();
                }
            }
        }
        else {
            while (this->queue.empty()) {
                this->empty_cond.wait(lock);
            }
        }
    }

    item = this->queue.front();
    this->queue.pop_front();
    lock.unlock();
    this->full_cond.notify_one();
}

template<typename T>
void
ConcurrentQueue<T>::task_done()
{
    boost::mutex::scoped_lock lock(this->mutex);

    if (this->unfinished_tasks == 0) {
        throw NoMoreTasks();
    }

    if (--this->unfinished_tasks == 0) {
        this->all_tasks_done_cond.notify_all();
    }
}

template<typename T>
void
ConcurrentQueue<T>::join()
{
    boost::mutex::scoped_lock lock(this->mutex);

    while (this->unfinished_tasks) {
        this->all_tasks_done_cond.wait(lock);
    }
}
#endif
