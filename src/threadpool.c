#include <stdio.h>
#include <pthread.h>
#include <errno.h>
#include <threads.h>
#include <stdlib.h>
#include "threadpool.h"
#include "list.h"

enum FUTURE_STATUS {DONE,INPROGRESS,LOADED}; //static enum?
static void* thread_run(void *_args);
/**
 * \brief struct that holds thread specific information for each thread.
 * 
 */
struct thread_storage
{
    bool intr_thread;
    struct list queue;
    pthread_mutex_t lock;
    struct thread_pool* pool;
    struct list_elem elem; //
};
static thread_local struct thread_storage* tls;
static struct list_elem* steal_task(struct thread_pool* pool);
struct thread_pool
{
    struct list queue;
    pthread_mutex_t pool_lock;
    pthread_cond_t pool_cond;
    bool shutdown;
    pthread_t *thread_ids;
    int thread_count;
    
    struct list threads; //list of threads
};
/**
 * \brief struct holds information of a future
 * \task the function to be run
 * \args arguments to be passed to the function
 * \result the return of the function after executing
 * \status enum holding the different states of the future
 * \fu_cond future condition variable
 * \fu_lock future lock 
 * \elem ??
 */
struct future
{
    fork_join_task_t task;
    void* args;
    void* result;
    enum FUTURE_STATUS status;
    struct list_elem elem;
    pthread_cond_t fu_cond;
    pthread_mutex_t fu_lock;
    struct thread_storage* parent_thread;
    
};


/**
 * \brief creates nthreads threads and intializes the thread_pool
 * 
 * \param nthreads number of threads to be created
 * \return struct thread_pool* 
 * \note the threadpool needs to be freed by calling thread_pool_shutdown_and_destroy
 */
struct thread_pool * thread_pool_new(int nthreads)
{
    struct thread_pool * pool = malloc(sizeof *pool);
    
    //intialize thread_pool variables
    tls = malloc(sizeof* tls); //intilizes thread storage for main thread
    tls->intr_thread=false;  //sets main thread as not being a main thread
    pthread_mutex_init(&tls->lock, NULL);

    pool -> thread_ids = malloc(nthreads* sizeof*pool->thread_ids); //stores thread ids in heap so that they can be freed later
    pool ->shutdown = false; //variables signaling shutdown of the thread_pool
    pool -> thread_count= nthreads; //number of threads 

    list_init(&pool->queue);
    pthread_mutex_init(&pool->pool_lock, NULL);
    pthread_cond_init(&pool->pool_cond, NULL);
    list_init(&pool->threads);

    for(int i = 0; i < nthreads; i++)
    {
        int rc = pthread_create(&pool->thread_ids[i], NULL, thread_run, pool);
        if (rc != 0) {
            errno = rc;
            perror("pthread_create");
        }
        

    }
    return pool;
}
/*
* Shutdown this thread pool in an orderly fashion.
* Tasks that have been submitted but not executed may or
* may not be executed.
*
* Deallocate the thread pool object before returning.
*/


/**
 * \brief shutsdown and joins all the threads. Frees the thread_pool and it's variables 
 * 
 * \param pool pointer to the thread_pool to be cleared 
 */
void thread_pool_shutdown_and_destroy(struct thread_pool * pool)
{
    pthread_mutex_lock(&pool->pool_lock);
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->pool_cond);
    pthread_mutex_unlock(&pool->pool_lock);

    for(int i =0; i< pool ->thread_count ; i++)
    {
        pthread_join(pool->thread_ids[i], NULL);
    }
    free(pool->thread_ids);
    free(pool);
    free(tls);

}

/*
* Submit a fork join task to the thread pool and return a
* future. The returned future can be used in future_get()
* to obtain the result.
* ’pool’ - the pool to which to submit
* ’task’ - the task to be submitted.
* ’data’ - data to be passed to the task’s function
*
* Returns a future representing this computation.
*/

/**
 * \brief Submits a task to a thread_pool. A future is contracted to contain info on the task and added to the queue
 * 
 * \param pool the pool the task should be added to
 * \param task the task to be run
 * \param data task args
 * \return struct future* returns a pointer to a promised future that will contain the return 
 * \note 
 * - think of when a future should be destroyed? wether it's internal or external;
 * - could add lock,cond and list variables in the beginning and assign them in the if for cleaner code
 * then have the code after the if take that lock,cond and list
 */

struct future * thread_pool_submit(struct thread_pool *pool, fork_join_task_t task, void * data)
{   
    //set up future for the task sumbitted 
    struct future *fut= malloc(sizeof *fut);
    fut -> status = LOADED; //sets the future to the loaded state (in a queue but not run)
    fut -> task = task;
    fut -> args = data;
    fut -> result = NULL;
    fut -> parent_thread = tls; //gives it the parent thread storage pointer

    //initials lock and condition associated with the following future;
    pthread_mutex_init(&fut->fu_lock, NULL);  
    pthread_cond_init(&fut->fu_cond, NULL); //(note) a question of where it's needed 
    //if task is from an intrenal thread then it's a child task and should be added to that thread's queue
    if(tls ->intr_thread)
    {
        //could define lock 
        pthread_mutex_lock(&tls->lock); //locks thread queue
        list_push_back(&tls->queue,&fut->elem);
        pthread_mutex_unlock(&tls->lock);//unlocks thread queue
        pthread_cond_broadcast(&pool->pool_cond);
        return fut;
    }

    pthread_mutex_lock(&pool->pool_lock); //lock queue to add
    list_push_front(&pool->queue, &fut ->elem); //global queue FIFO?
    //broadcasts that a variable added to the global queue. Used for now as threads that have nothing to do are blocked. Could be used in the future for threads that have nothing to do
    pthread_cond_broadcast(&pool->pool_cond); 
    pthread_mutex_unlock(&pool->pool_lock);
 
    return fut;

}

/* Make sure that the thread pool has completed the execution
* of the fork join task this future represents.
*
* Returns the value returned by this task.
*/
void * future_get(struct future * fut)
{
    if(tls->intr_thread)
    {
        while(1)
        {
            pthread_mutex_lock(&fut->fu_lock);
            if(fut-> status == DONE)
            {
                pthread_mutex_unlock(&fut->fu_lock);
                 //break;
                 return fut ->result;
            }
            pthread_mutex_unlock(&fut->fu_lock);

            struct future* new_fut = NULL;
            pthread_mutex_lock(&fut->parent_thread->lock);
            if(!list_empty(&fut->parent_thread->queue)  && fut->status != DONE)
            {
                struct list_elem* e = list_pop_back(&fut->parent_thread->queue);
                new_fut = list_entry(e, struct future, elem);

            }
            pthread_mutex_unlock(&fut->parent_thread->lock);

            if(new_fut)
            {
                pthread_mutex_lock(&new_fut->fu_lock);
                new_fut->status = INPROGRESS;
                pthread_mutex_unlock(&new_fut->fu_lock);

                new_fut->result = new_fut->task(new_fut->parent_thread->pool,new_fut->args); // with work stealing the pool passed down should be the cuurent's threads pool
                pthread_mutex_lock(&new_fut->fu_lock);
                new_fut->status = DONE;
                //pthread_cond_signal(&new_fut->fu_cond); // commenting might cause issues in the waits below
                pthread_mutex_unlock(&new_fut->fu_lock);
                
            }
        }

    }

    pthread_mutex_lock(&fut->fu_lock);
    enum FUTURE_STATUS state = fut -> status;
    //while(state != DONE)
    //{
        switch(state)
        {
            case LOADED: //what should happen if future is called but task isn't still LOADED?
                pthread_cond_wait(&fut-> fu_cond, &fut->fu_lock); //error
                break;


            case INPROGRESS: //Same as above?
                pthread_cond_wait(&fut-> fu_cond, &fut->fu_lock);
                break;

            case DONE:
                break;
                

        }
    //}
    pthread_mutex_unlock(&fut->fu_lock);
    return fut->result;
    //brodacast to signal 
    //sempphores not cond
    //result save to dummy then run 

}

/* Deallocate this future. Must be called after future_get() */
void future_free(struct future * fut)
{
    free(fut);

}

static void* thread_run(void *_args)
{
    struct thread_pool* pool =(struct thread_pool*) _args;
    tls = malloc(sizeof *tls);
    tls ->intr_thread= true; //identifies that this thread is internal
    tls ->pool = pool;

    pthread_mutex_init(&tls->lock, NULL);
    list_init(&tls->queue);
    
    
    struct list_elem* e = NULL;
    struct future* fut = NULL;
    pthread_mutex_lock(&pool->pool_lock);
    list_push_front(&tls->pool->threads, &tls->elem);
    pthread_mutex_unlock(&pool->pool_lock);
    while(1)
    {
        pthread_mutex_lock(&pool->pool_lock); //locks pool to manipulate it
        if(pool ->shutdown == true) break;
        bool empty = list_empty(&pool->queue);
        if(!empty)
        {

            e = list_pop_back(&pool->queue);
            pthread_mutex_unlock(&pool->pool_lock);
            fut = list_entry(e, struct future, elem); 
            pthread_mutex_lock(&fut->fu_lock);
            fut->parent_thread = tls;
            fut->status = INPROGRESS;
            pthread_mutex_unlock(&fut->fu_lock);
            fut->result = fut->task(pool,fut->args); // with work stealing the pool passed down should be the cuurent's threads pool
            

            pthread_mutex_lock(&fut->fu_lock);
            fut->status = DONE;
            pthread_cond_signal(&fut->fu_cond);
            pthread_mutex_unlock(&fut->fu_lock);
            continue;
        }
        else if(0)
        {
            e = steal_task(tls->pool);
            pthread_mutex_unlock(&pool->pool_lock);
            fut = list_entry(e, struct future, elem); 
            pthread_mutex_lock(&fut->fu_lock);
            fut->parent_thread = tls;
            fut->status = INPROGRESS;
            pthread_mutex_unlock(&fut->fu_lock);
            fut->result = fut->task(pool,fut->args);
            

            pthread_mutex_lock(&fut->fu_lock);
            fut->status = DONE;
            pthread_cond_broadcast(&fut->fu_cond);
            pthread_mutex_unlock(&pool->pool_lock);
            continue;
        }
        pthread_cond_wait(&pool->pool_cond,&pool->pool_lock);
        pthread_mutex_unlock(&pool->pool_lock);
    }
    pthread_mutex_unlock(&pool->pool_lock);
    free(tls);
    //put everything in the if 
    //wait at the very end 

    return NULL;
    
}

//struct future* run_task()


/**
 * \brief Steals a task from another thread if its queue is empty
 * 
 * \param pool that has the list of threads
 * \return list_elem of the task being popped 
 */
static struct list_elem* steal_task(struct thread_pool* pool) {
    struct list_elem* elem = NULL;
    for (struct list_elem * i = list_begin(&pool->threads); i != list_end(&pool->threads); i = list_next(i)) {
        struct thread_storage * ts = list_entry(i, struct thread_storage, elem);

        pthread_mutex_lock(&ts->lock);
        if (!list_empty(&tls->queue)) {
           elem = list_pop_back(&ts->queue);
           break;
        }
    }
    pthread_mutex_unlock(&tls->lock);
    return elem;
}
