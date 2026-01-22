#ifndef BANK_H
#define BANK_H

#include <stdbool.h>
#include <stddef.h>

typedef struct bank bank_t;

bank_t *bank_new(size_t n);

void bank_free(bank_t *bank);

void deposit(bank_t *bank, int id, int amt);

bool withdraw(bank_t *bank, int id, int amt);

int check_balance(bank_t *bank, int id);

void transfer(bank_t *bank, int src, int dst, int amt);

bool wait_balance(bank_t *bank, int id, bool (*alert_fxn)(int));

#endif
