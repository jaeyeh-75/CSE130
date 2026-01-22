#include "bank.h"
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

typedef struct {
    int balance;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} account_t;

struct bank {
    size_t n_accounts;
    account_t *accounts;
    bool is_freed;
    pthread_mutex_t free_lock;
};

bank_t *bank_new(size_t n) {
    bank_t *bank = malloc(sizeof(bank_t));
    if (!bank)
        return NULL;

    bank->n_accounts = n;
    bank->accounts = malloc(n * sizeof(account_t));
    bank->is_freed = false;

    if (!bank->accounts) {
        free(bank);
        return NULL;
    }

    pthread_mutex_init(&bank->free_lock, NULL);

    for (size_t i = 0; i < n; i++) {
        bank->accounts[i].balance = 0;
        pthread_mutex_init(&bank->accounts[i].lock, NULL);
        pthread_cond_init(&bank->accounts[i].cond, NULL);
    }

    return bank;
}

void bank_free(bank_t *bank) {
    if (!bank)
        return;

    pthread_mutex_lock(&bank->free_lock);
    bank->is_freed = true;
    pthread_mutex_unlock(&bank->free_lock);

    for (size_t i = 0; i < bank->n_accounts; i++) {
        pthread_mutex_lock(&bank->accounts[i].lock);
        pthread_cond_broadcast(&bank->accounts[i].cond);
        pthread_mutex_unlock(&bank->accounts[i].lock);
    }

    for (size_t i = 0; i < bank->n_accounts; i++) {
        pthread_mutex_destroy(&bank->accounts[i].lock);
        pthread_cond_destroy(&bank->accounts[i].cond);
    }

    pthread_mutex_destroy(&bank->free_lock);
    free(bank->accounts);
    free(bank);
}

void deposit(bank_t *bank, int id, int amt) {
    pthread_mutex_lock(&bank->accounts[id].lock);
    bank->accounts[id].balance += amt;
    pthread_cond_broadcast(&bank->accounts[id].cond);
    pthread_mutex_unlock(&bank->accounts[id].lock);
}

bool withdraw(bank_t *bank, int id, int amt) {
    pthread_mutex_lock(&bank->accounts[id].lock);

    if (bank->accounts[id].balance >= amt) {
        bank->accounts[id].balance -= amt;
        pthread_cond_broadcast(&bank->accounts[id].cond);
        pthread_mutex_unlock(&bank->accounts[id].lock);
        return true;
    }

    pthread_mutex_unlock(&bank->accounts[id].lock);
    return false;
}

int check_balance(bank_t *bank, int id) {
    pthread_mutex_lock(&bank->accounts[id].lock);
    int balance = bank->accounts[id].balance;
    pthread_mutex_unlock(&bank->accounts[id].lock);
    return balance;
}

void transfer(bank_t *bank, int src, int dst, int amt) {
    int first;
    int second;

    if (src < dst) {
        first = src;
        second = dst;
    } else {
        first = dst;
        second = src;
    }

    if (src == dst) {
        return;
    }

    pthread_mutex_lock(&bank->accounts[first].lock);
    pthread_mutex_lock(&bank->accounts[second].lock);

    if (bank->accounts[src].balance >= amt) {
        bank->accounts[src].balance -= amt;
        bank->accounts[dst].balance += amt;

        pthread_cond_broadcast(&bank->accounts[src].cond);
        pthread_cond_broadcast(&bank->accounts[dst].cond);
    }

    pthread_mutex_unlock(&bank->accounts[second].lock);
    pthread_mutex_unlock(&bank->accounts[first].lock);
}

bool wait_balance(bank_t *bank, int id, bool (*alert_fxn)(int)) {
    pthread_mutex_lock(&bank->accounts[id].lock);

    while (true) {
        pthread_mutex_lock(&bank->free_lock);
        bool freed = bank->is_freed;
        pthread_mutex_unlock(&bank->free_lock);

        if (freed) {
            pthread_mutex_unlock(&bank->accounts[id].lock);
            return false;
        }

        if (alert_fxn(bank->accounts[id].balance)) {
            pthread_mutex_unlock(&bank->accounts[id].lock);
            return true;
        }

        pthread_cond_wait(&bank->accounts[id].cond, &bank->accounts[id].lock);
    }
}
