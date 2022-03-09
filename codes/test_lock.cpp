//
// Created by prince_de_marcia on 2021/7/20.
//

#include "util.h"
#include "config.h"

Pri_lock lck;
int flag;
pthread_t thd[20];

void *func(void * id)
{
    int thd_id = *(int *)id;
    printf("thd%d ready\n", thd_id);
    while( flag == 0 );
    for( int i = 0; i < 4+thd_id*3; i ++ ){
        int tmp = thd_id+1;
        lck.lock( thd_id, tmp );
        usleep(100000);
        printf("thd_id %d pri %d iter %d\n", thd_id, tmp, i);
        lck.unlock( thd_id );
    }
}

int main()
{
    int a[20];
    lck.init( 10 );
    flag = 0;
    for( int i = 0; i < 10; i ++ ){
        a[i] = i;
        pthread_create( &thd[i], NULL, func, &a[i] );
    }
    sleep(1);
    flag = 1;
    sleep(20);
}
