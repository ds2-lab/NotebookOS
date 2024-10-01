add_checked_function(mod, 'smr_NewBytes', retval('uint64_t'), [param('char*', 'bytes', transfer_ownership=False), param('int', 'len')])

mod.generate(open('smr.c', 'w'))