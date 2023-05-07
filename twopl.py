class LockManager:
    def __init__(self):
        self.resource_locks = {}
        self.waiting = {}

    def check_conflict(self, resource, lock_type, transaction):
        if resource not in self.resource_locks:
            self.resource_locks[resource] = []
            return False

        existing_locks = self.resource_locks[resource]

        for (existing_lock_type, existing_transaction) in existing_locks:
            if existing_transaction.tid == transaction.tid:
                continue
            if lock_type == 'R' and existing_lock_type == 'R':
                continue
            return True

        return False

    def add_lock(self, resource, lock_type, transaction):
        if(not self.check_conflict(resource, lock_type, transaction)):
            self.resource_locks.setdefault(resource, []).append((lock_type, transaction))
            transaction.locked_resources.add(resource)
            return True
        self.waiting.setdefault(resource, []).append(transaction)
        return False

    def release_lock(self, resource, transaction):
        self.resource_locks[resource] = [(lock_type, existing_transaction) for (lock_type, existing_transaction) in self.resource_locks[resource] if existing_transaction.tid != transaction.tid]
        for transaction in self.waiting[resource]:
            transaction.waiting = False



class Transaction:
    def __init__(self, tid):
        self.tid = tid
        self.locked_resources = set()
        self.resources_needed = set()
        self.operations = []
        self.finished_operations = []
        self.growing = True
        self.waiting = False
        self.killed = False

    def all_locks_acquired(self):
        if not self.growing:
            return True
        all_acquired = len(self.locked_resources) == len(set(self.resources_needed))
        if all_acquired:
            self.check_growing()
        return all_acquired

    def check_growing(self):
        if self.growing:
            self.growing = False

    def release_locks(self, results, lock_manager):  # Add lock_manager as a parameter
        if not self.growing:
            for operation in self.finished_operations:
                resource = operation[1]
                if resource in self.locked_resources:
                    resource_needed = False
                    for x in self.operations:
                         if resource==x[1]:
                             resource_needed =  True
                    if not resource_needed:
                        self.locked_resources.remove(resource)
                        results.append(f"ul{self.tid}({resource})")
                        lock_manager.release_lock(resource, self)  # Inform the LockManager



    def get_lock_on_resource(self, resource,lock_manager):
        required_lock_type = self.get_required_lock_type(resource)
        if lock_manager.add_lock(resource,required_lock_type, self):
            return True
        else:
            self.waiting = True
            return False

    def get_required_lock_type(self, resource):
        required_lock_type = 'R'
        for operation in self.operations:
            if operation[1] == resource and operation[0] == 'W':
                required_lock_type = 'W'
                break
        return required_lock_type

    def __str__(self):
            locked_resources_str = ', '.join(self.locked_resources)
            resources_needed_str = ', '.join(self.resources_needed)
            operations_str = '\n'.join([f'{op[0]} {op[1]}' for op in self.operations])
            finished_operations_str = '\n'.join([f'{op[0]} {op[1]}' for op in self.finished_operations])
            return (f'Transaction {self.tid}\n'
                    f'Locked resources: {locked_resources_str}\n'
                    f'Resources needed: {resources_needed_str}\n'
                    f'Operations:\n{operations_str}\n'
                    f'Finished operations:\n{finished_operations_str}\n'
                    f'Growing: {self.growing}\n'
                    f'Waiting: {self.waiting}\n'
                    f'Killed: {self.killed}')

def parse_operations(operations_str):
    operations = operations_str.split()
    transactions = {}

    for op in operations:
        action = op[0]
        tid = int(op[1])
        resource = op[3:-1]  # Updated to exclude parentheses

        if tid not in transactions:
            transactions[tid] = Transaction(tid)

        transaction = transactions[tid]
        transaction.operations.append((action, resource))
        transaction.resources_needed.add(resource)

    return transactions

def run_two_phase_locking(operations_str):
    transactions = parse_operations(operations_str)
    lock_manager = LockManager()
    results = []
    operations = operations_str.split();
    while operations:
        i = 0
        deadlock = False
        while operations:
            op_str = operations[i]
            action = op_str[0]
            tid = int(op_str[1])
            resource = op_str[3:-1]
            transaction = transactions[tid]
            # print(op_str,transaction)
            if not transaction.waiting:
                if resource in transaction.locked_resources:
                    results.append(f"{action}{tid}({resource})")
                    transaction.operations.pop(0)
                    transaction.finished_operations.append((action, resource))
                    operations.pop(i)
                    transaction.release_locks(results, lock_manager)
                    break;
                elif transaction.get_lock_on_resource(resource,lock_manager):
                    lock_type = transaction.get_required_lock_type(resource)
                    results.append(f"{lock_type.lower()}l{tid}({resource})")
                    if transaction.all_locks_acquired():
                        transaction.release_locks(results, lock_manager)
                        break;
                    results.append(f"{action}{tid}({resource})")
                    transaction.operations.pop(0)
                    transaction.finished_operations.append((action, resource))
                    operations.pop(i)
                    break;

            else:
                i=i+1
                if(i>=len(operations)):
                    results.append("deadlock occurs")
                    deadlock = True
                    break;
            # print(operations)
            # print(results)
        if(deadlock):
            break

    return " ".join(results)


input_str = "R1(X) R3(Y) W1(X) R2(X) W2(X) R1(Y) R2(Z) W3(Z) W1(Y) W2(Y)"
expected_output = "wl1(X) R1(X) rl3(Y) R3(Y) W1(X) wl3(Z) ul3(Y) wl1(Y) ul1(X) wl2(X) R2(X) W2(X) R1(Y) W3(Z) ul3(Z) rl2(Z) R2(Z) W1(Y) ul1(Y) wl2(Y) ul2(X) ul2(Z) W2(Y) ul2(Y)"
result = run_two_phase_locking(input_str)
assert result == expected_output
input_str = "W3(C) R2(B) W2(B) R1(A) R3(B) R2(C) W1(A) W2(A) R1(B) W1(B)"
expected_output = "wl3(C) W3(C) wl2(B) R2(B) W2(B) wl1(A) R1(A) W1(A) deadlock occurs"
result = run_two_phase_locking(input_str)
assert result == expected_output
input_str = "R1(X) W3(Z) R2(X) W1(X) W2(X) R2(Z) R1(Y) R3(Y) W1(Y) W2(Y)"
expected_output = "wl1(X) R1(X) wl3(Z) W3(Z) W1(X) wl1(Y) ul1(X) wl2(X) R2(X) W2(X) R1(Y) W1(Y) ul1(Y) rl3(Y) ul3(Z) rl2(Z) R2(Z) R3(Y) ul3(Y) wl2(Y) ul2(X) ul2(Z) W2(Y) ul2(Y)"
result = run_two_phase_locking(input_str)
assert result == expected_output
input_str = "R1(X) W3(Z) R2(X) W1(X) W2(X) R2(Z) R1(Y) R3(Y) W1(Y) W2(Y)"
result = run_two_phase_locking(input_str)
print(input_str,"\n",result)
