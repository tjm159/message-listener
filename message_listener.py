import os
import time
import argparse
import importlib

import numpy as np


def clear_console():
    os.system('cls' if os.name=='nt' else 'clear')


def format_output(msg_type, times):
    if np.size(times) >= 2:
        nptimes = np.array(times, dtype='uint64')
        deltas = np.diff(nptimes)
        avg = np.mean(deltas) / 1e6
        high = np.max(deltas) / 1e6
        low = np.min(deltas) / 1e6

        return (f'{msg_type}: Count: {np.size(times)}, ' + 
                f'Rate: {1e3/avg:.3f} Hz, ' + 
                f'Avg: {avg:.3f} ms, Max: {high:.3f} ms, Min: {low:.3f} ms')
    else:
        return f'{msg_type}: Count: {np.size(times)}'


def run(msg_sys, msg_sys_mod, msg_defs, update_rate):

    # Get all message types from dragonfly and message definitions
    msg_types = { **{getattr(msg_defs, item) : item 
                     for item in dir(msg_defs) if item.startswith('MT_')},
                  **{getattr(msg_sys, item) : item
                     for item in dir(msg_sys) if item.startswith('MT_')} }

    msg_times = dict()

    mod = msg_sys_mod(0, 0)
    mod.ConnectToMMM('localhost:7111')
    mod.Subscribe(msg_sys.ALL_MESSAGE_TYPES)

    print('Message Listener is running...')

    last_update = 0

    while (1):

        msg = msg_sys.CMessage()
        count = mod.ReadMessage(msg, timeout=0)

        if count > 0:
            # Get read message
            msg_type = msg.GetHeader().msg_type
            msg_type_name = msg_types.get(msg_type)
            if msg_type_name:
                if msg_type_name in msg_times:
                    msg_times[msg_type_name].append(time.perf_counter_ns())
                else:
                    msg_times[msg_type_name] = [time.perf_counter_ns()]

        if (time.perf_counter() - last_update) > update_rate: 
            clear_console()
            if msg_times:
                output = '\n'.join([format_output(key, value) 
                                    for (key, value) in msg_times.items()])
                print(output)
                msg_times = {key: list() for key in msg_times.keys()}
            else:
                print('No messages received')

            last_update = time.perf_counter()

    mod.DisconnectFromMMM()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--rate', 
                        type=int,
                        default=5,
                        help='Console update period')
    parser.add_argument('-s', '--msgsys',
                        type=str,
                        choices=['dragonfly', 'rtma'],
                        default='dragonfly',
                        help='Messaging system')
    parser.add_argument('-m', '--msgdefs', 
                        type=str,
                        default='HX_message_defs',
                        help='Message definitions module')
    return parser.parse_args()


def import_mods(args):
    if args.msgsys.lower() == 'dragonfly':
        msg_sys_lib = importlib.import_module('pydragonfly') 
        msg_sys_mod = msg_sys_lib.Dragonfly_Module 
    elif args.msgsys.lower() == 'rtma':
        msg_sys_lib = importlib.import_module('PyRTMA3') 
        msg_sys_mod = msg_sys_lib.RTMA_Module 
    else:
        raise Exception(f'Unsupported messaging system {args.msgsys}')

    msg_defs = importlib.import_module(args.msgdefs)

    return {'msg_sys': msg_sys_lib,
            'msg_sys_mod': msg_sys_mod,
            'msg_defs': msg_defs}


if __name__ == '__main__':
   args = parse_args()
   mods = import_mods(args=args)
   run(msg_sys=mods['msg_sys'],
       msg_sys_mod=mods['msg_sys_mod'],
       msg_defs=mods['msg_defs'],
       update_rate=args.rate) 
